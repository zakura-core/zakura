//! `cargo xtask sim-acceptance --subject <S> --header-out <dir> --out <dir>
//! [--deadline-secs <n>] [--only <text>]...`:
//! sim-7's acceptance run (the sim-7 plan, "sim-7.4's tool").
//!
//! It keeps the shared runner's rules ([`crate::acceptance`]): one run at a
//! time per `--out`, a fresh run directory, a clean checkout of S inside it,
//! a deadline on every child, containment, and a record that exists only if
//! the run fully succeeded. Every binary it runs is its own build from that
//! checkout, in a target directory of its own (Rule 5).
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
        remove_worktrees_under, run_id, source_digest, stop_if_interrupted, track_sessions,
        Options, Tool, ESCAPED, RECORD, RUNS, SOURCE_DIGEST, SUBJECT_DIR,
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
const RUN_TARGETS: [&str; 6] = [
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
    let args = args(args_in)?;
    let options = Options {
        out: args.out.clone(),
        deadline: args.deadline,
    };
    fs::create_dir_all(&args.out)?;
    let (_lock, kept) = lock_out(&TOOL, &args.out)?;
    mark_descendants(&TOOL, &args.out);
    let mut sessions = track_sessions(&args.out);
    let contained = contain();
    let source = subject_source(repo_root, &options, &args.subject)?;
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
    let result = run_at(&root, &run, &args, &source, contained);
    let removed = remove_worktrees_under(repo_root, &run, &run_dir);
    let summary = result?;
    removed?;
    stop_if_interrupted()?;
    if ESCAPED.load(Ordering::Relaxed) {
        return Err(
            "a child's descendants couldn't be confirmed gone, so the record can't be trusted"
                .into(),
        );
    }
    keep_executables(&run_dir)?;
    for target in RUN_TARGETS {
        let target = run_dir.join(target);
        if target.exists() {
            fs::remove_dir_all(target)?;
        }
    }
    publish_record(&args.out, &run_dir)?;
    println!("{summary}");
    println!("record: {}", args.out.join(RECORD).display());
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

    // Every seed, in one report, then each harness's full inputs written to
    // one corpus by their SHA-256 (`--out` holds one harness's seeds).
    let corpus = run.out.join("corpus");
    let mut seeds = Command::new(&release);
    seeds
        .arg("seeds")
        .arg("--report")
        .arg(run.out.join("seeds.json"))
        .current_dir(root);
    let _ = child(&mut seeds, &logs.join("seeds.log"), run.deadline);
    let report: Value = serde_json::from_str(
        &fs::read_to_string(run.out.join("seeds.json")).unwrap_or_else(|_| "{}".to_owned()),
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
        fill.args(["seeds", "--sim", harness, "--out"])
            .arg(&corpus)
            .current_dir(root);
        let log = logs.join(format!("seeds-{}.log", harness.replace('/', "-")));
        let _ = child(&mut fill, &log, run.deadline);
    }
    let seed_of = |harness: &str| -> Option<PathBuf> {
        report.get("seeds")?.as_array()?.iter().find_map(|row| {
            if row.pointer("/sidecar/harness")?.as_str()? != harness {
                return None;
            }
            Some(corpus.join(row.get("input_sha256")?.as_str()?))
        })
    };

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
        fs::write(dir.join("task.json"), serde_json::to_string_pretty(task)?)?;
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
        };
        match kind.as_str() {
            "test" => run_test(
                root,
                run,
                Some(source),
                fields,
                &dir,
                &run.out.join("tests"),
            )?,
            "fuzz-test" => ctx.fuzz_test(fields)?,
            "lane" => ctx.lane(fields)?,
            "seed" => ctx.seed(fields)?,
            "seed-witness" => ctx.seed_witness(fields)?,
            "command" => ctx.command(fields)?,
            "measure" => ctx.measure(fields, &seed_of)?,
            other => return Err(format!("the plan has an unknown task kind `{other}`").into()),
        }
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
}

/// `field` of a task, as text.
fn field<'v>(task: &'v Value, field: &str) -> Result<&'v str, BoxError> {
    task.get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("the task has no `{field}`").into())
}

impl Ctx<'_> {
    /// Runs `command` under the run's deadline with its output in
    /// `<dir>/<name>.log`, and writes `<dir>/<name>.json` with whether it
    /// exited 0, whether it was killed, and its output.
    fn record(&self, command: &mut Command, name: &str) -> Result<String, BoxError> {
        let log = self.dir.join(format!("{name}.log"));
        let ended = child(command, &log, self.run.deadline);
        stop_if_interrupted()?;
        let text = fs::read_to_string(&log).unwrap_or_default();
        fs::write(
            self.dir.join(format!("{name}.json")),
            serde_json::to_string_pretty(&json!({
                "exited_ok": ended.exited(),
                "killed": ended.killed(),
                "log": text,
            }))?,
        )?;
        Ok(text)
    }

    /// One test of the `qa/fuzz/sim` workspace's library, by its exact
    /// name, without the ASan-only default feature. `test.json` has what
    /// libtest reported.
    fn fuzz_test(&self, task: &Value) -> Result<(), BoxError> {
        let test = field(task, "test")?;
        let mut cargo = cargo(self.root, &self.run.out.join("fuzz"));
        cargo.args([
            "test",
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
        fs::write(
            self.dir.join("test.json"),
            serde_json::to_string_pretty(&json!({
                "tests": libtest(&text),
                "killed": ended.killed(),
                "exited_ok": ended.exited(),
                "log": text,
            }))?,
        )?;
        Ok(())
    }

    /// One lane's command, through `sh -c` as `qa/suite.toml` spells it,
    /// then each test binary it ran listed with `--list`, so `lane.json`
    /// says which listed test didn't run and pass (Rule 11).
    fn lane(&self, task: &Value) -> Result<(), BoxError> {
        let command = field(task, "command")?;
        let package = command
            .split_whitespace()
            .skip_while(|word| *word != "-p")
            .nth(1)
            .unwrap_or_default()
            .to_owned();
        let mut sh = Command::new("sh");
        sh.args(["-c", command]).current_dir(self.root);
        let log = self.dir.join("lane.log");
        let ended = child(&mut sh, &log, self.run.deadline);
        stop_if_interrupted()?;
        let text = fs::read_to_string(&log).unwrap_or_default();
        let mut targets: BTreeMap<String, Vec<(String, String)>> = BTreeMap::new();
        let mut binaries: Vec<(String, PathBuf)> = Vec::new();
        let mut current = String::new();
        let mut section = String::new();
        let flush = |target: &str, section: &str, targets: &mut BTreeMap<_, Vec<_>>| {
            if !target.is_empty() {
                targets
                    .entry(target.to_owned())
                    .or_default()
                    .extend(libtest(section));
            }
        };
        for line in text.lines() {
            if let Some(rest) = line.trim().strip_prefix("Running ") {
                flush(&current, &section, &mut targets);
                section.clear();
                current = lane_target(&package, rest);
                if let Some(path) = rest.rsplit_once('(').map(|(_, p)| p.trim_end_matches(')')) {
                    binaries.push((current.clone(), self.root.join(path)));
                }
            } else {
                section.push_str(line);
                section.push('\n');
            }
        }
        flush(&current, &section, &mut targets);
        let mut unrun = Vec::new();
        for (target, binary) in &binaries {
            let (Some(listed), Some(ignored)) = (
                list_tests(binary, false, self),
                list_tests(binary, true, self),
            ) else {
                unrun.push(format!("{target}: its binary's --list failed"));
                continue;
            };
            let ran: BTreeSet<&str> = targets
                .get(target)
                .map(|tests| {
                    tests
                        .iter()
                        .filter(|(_, outcome)| outcome == "ok")
                        .map(|(name, _)| name.as_str())
                        .collect()
                })
                .unwrap_or_default();
            unrun.extend(
                listed
                    .difference(&ignored)
                    .filter(|name| !ran.contains(name.as_str()))
                    .map(|name| format!("{target}::{name}")),
            );
        }
        fs::write(
            self.dir.join("lane.json"),
            serde_json::to_string_pretty(&json!({
                "exited_ok": ended.exited(),
                "killed": ended.killed(),
                "targets": targets,
                "unrun": unrun,
                "log": text,
            }))?,
        )?;
        Ok(())
    }

    /// One committed seed's full input recorded with the release build,
    /// then replayed fresh by the same build: `trace/` and `replay.json`.
    fn seed(&self, task: &Value) -> Result<(), BoxError> {
        let mut record = self.zakura_sim(task, &[])?;
        self.record(&mut record, "record")?;
        let mut replay = Command::new(self.release);
        replay
            .arg("replay")
            .arg(self.dir.join("trace"))
            .current_dir(self.root);
        self.record(&mut replay, "replay")?;
        Ok(())
    }

    /// One seed witness, recorded with `--witness`: `trace/` with its
    /// `witness.json`.
    fn seed_witness(&self, task: &Value) -> Result<(), BoxError> {
        let property = field(task, "property")?;
        let expect = field(task, "expect")?;
        let mut record = self.zakura_sim(task, &["--witness", property, "--expect", expect])?;
        self.record(&mut record, "record")?;
        let mut replay = Command::new(self.release);
        replay
            .arg("replay")
            .arg(self.dir.join("trace"))
            .current_dir(self.root);
        self.record(&mut replay, "replay")?;
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
        let mut cargo = cargo(self.root, &self.run.out.join("commands"));
        if !args.iter().any(|arg| arg == "zakura-sim") {
            cargo.env_remove("RUSTFLAGS");
        }
        cargo.args(&args);
        self.record(&mut cargo, "command")?;
        Ok(())
    }

    /// One measured recording of `harness`, on its first committed seed or
    /// on empty input if it has none: `trace/` and `measure.json`.
    fn measure(
        &self,
        task: &Value,
        seed_of: &dyn Fn(&str) -> Option<PathBuf>,
    ) -> Result<(), BoxError> {
        let harness = field(task, "harness")?;
        let input = match seed_of(harness) {
            Some(input) => input,
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
        self.record(&mut record, "record")?;
        Ok(())
    }
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

/// A lane's target name from cargo's `Running` line: `<package>/lib` for
/// the library's unit tests, `<package>/<stem>` for an integration test.
fn lane_target(package: &str, running: &str) -> String {
    let what = running.split(" (").next().unwrap_or(running).trim();
    if what.starts_with("unittests") {
        return format!("{package}/lib");
    }
    let stem = Path::new(what)
        .file_stem()
        .map_or_else(String::new, |stem| stem.to_string_lossy().into_owned());
    format!("{package}/{stem}")
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
    use super::lane_target;

    #[test]
    fn running_lines_name_their_targets() {
        assert_eq!(
            lane_target(
                "zakura-sim",
                "unittests src/lib.rs (target/sim-release/release/deps/zakura_sim-1)"
            ),
            "zakura-sim/lib"
        );
        assert_eq!(
            lane_target(
                "zakura-sim",
                "tests/draws.rs (target/sim-release/release/deps/draws-2)"
            ),
            "zakura-sim/draws"
        );
    }
}
