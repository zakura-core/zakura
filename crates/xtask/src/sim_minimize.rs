//! `cargo xtask sim-minimize`: reduce a simulation fuzz input with
//! `cargo fuzz tmin`, and keep a reduction only if fresh processes confirm
//! that it reproduces the original's failure (traces implementation plan,
//! T2b).
//!
//! Under `ZAKURA_SIM_EXPECT`, the fuzz target panics only on the expected
//! property's `Active` violation. An abort, a sanitizer report or a timeout
//! still crashes it, and `tmin` keeps any crash, so its output is only a
//! candidate. The wrapper:
//!
//! 1. copies the original into `<out>/original/` and checks the copy's
//!    SHA-256 before it starts any child;
//! 2. builds the simulator binary and the fuzz target once, and pins both by
//!    copying them into `<out>/bin/`;
//! 3. confirms the original in fresh processes ([`check_input`]);
//! 4. runs `cargo fuzz tmin` with the filter, `--runs` attempts per step and
//!    a `--wall-secs` limit;
//! 5. confirms each smaller candidate the same way, smallest first, and
//!    against the original's confirmation, and accepts the first that
//!    passes;
//! 6. writes `<out>/report.json`.
//!
//! A confirmation runs three fresh processes, each within `--child-secs`:
//! the pinned fuzz target must crash with the property's panic alone,
//! `zakura-sim confirm` must confirm the property, its trigger history, the
//! assumptions and the build, and `zakura-sim replay` must replay the
//! recorded trace Exact.
//!
//! The original's copy is written first, and an accepted candidate moves to
//! `<out>/minimized/` as soon as it passes, so an interrupt keeps both.

use std::{
    env,
    ffi::OsString,
    fmt, fs,
    io::{self, Read as _},
    path::{Path, PathBuf},
    process::{Command, ExitStatus, Stdio},
    sync::atomic::{AtomicBool, Ordering},
    thread,
    time::{Duration, Instant},
};

use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{header_fuzz::PINNED_NIGHTLY, sim::SIM_RUSTFLAGS, BoxError};

/// Each simulation fuzz target and the registered harness it runs. A target
/// must also be a `[[bin]]` of `qa/fuzz/sim/Cargo.toml`.
const TARGETS: [(&str, &str); 2] = [
    ("discovery_wait_complete", "discovery/wait-complete"),
    ("race_fixtures", "fixtures/all"),
];

/// The variable the fuzz target's filter reads (`qa/fuzz/sim/src/fuzz.rs`).
const EXPECT_VAR: &str = "ZAKURA_SIM_EXPECT";

/// The file `zakura-sim confirm` writes into its `--out`.
const CONFIRMATION_FILE: &str = "confirmation.json";

/// The report's file in the output directory.
pub(super) const REPORT_FILE: &str = "report.json";

/// The report's format version.
const REPORT_FORMAT: u32 = 1;

/// How long past `--child-secs` the fuzz target may run before the wrapper
/// kills it, so libFuzzer's own timeout report comes first.
const FUZZ_GRACE: Duration = Duration::from_secs(10);

/// How often the wrapper checks a child, the deadline and the interrupt.
const POLL: Duration = Duration::from_millis(20);

/// Set by SIGINT or SIGTERM.
static INTERRUPTED: AtomicBool = AtomicBool::new(false);

/// What the caller asked for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Request {
    /// The fuzz target.
    pub(super) target: String,
    /// The registered harness the target runs.
    pub(super) harness: String,
    /// The original crash input.
    pub(super) input: PathBuf,
    /// The property the original violates.
    pub(super) property: String,
    /// The output directory. It must be missing or empty.
    pub(super) out: PathBuf,
    /// The limits.
    pub(super) budget: Budget,
    /// The fuzz build's sanitizer.
    pub(super) sanitizer: Sanitizer,
}

/// The wrapper's limits. Each is given explicitly.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub(super) struct Budget {
    /// `tmin`'s minimization attempts per step, its `-runs`.
    pub(super) runs: u32,
    /// The longest `tmin` may run.
    #[serde(rename = "wall_ms", serialize_with = "millis")]
    pub(super) wall: Duration,
    /// The longest one confirmation child may run, and libFuzzer's
    /// `-timeout` for one input.
    #[serde(rename = "child_ms", serialize_with = "millis")]
    pub(super) child: Duration,
}

/// The fuzz build's sanitizer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(super) enum Sanitizer {
    /// cargo-fuzz's default AddressSanitizer build, with the scheduler's
    /// `sanitizer` feature.
    Address,
    /// No sanitizer, with the package's default features off.
    None,
}

impl Sanitizer {
    /// The `cargo fuzz` flags that select it.
    fn flags(self) -> &'static [&'static str] {
        match self {
            Sanitizer::Address => &["-s", "address"],
            Sanitizer::None => &["-s", "none", "--no-default-features"],
        }
    }
}

/// `cargo xtask sim-minimize`: parses `args`, builds and pins the tools,
/// minimizes, and prints where the report and the bundle are. Fails unless
/// a candidate was accepted.
pub(super) fn run(repo_root: &Path, args: impl Iterator<Item = String>) -> Result<(), BoxError> {
    let request = parse(repo_root, args)?;
    install_interrupt_handler();
    let report = minimize(repo_root, &request, &INTERRUPTED)?;
    finish(&request, &report)
}

/// Prints the outcome and returns it as the command's result.
fn finish(request: &Request, report: &Report) -> Result<(), BoxError> {
    let report_path = request.out.join(REPORT_FILE);
    match (&report.status, &report.minimized) {
        (Status::Minimized, Some(minimized)) => {
            println!(
                "minimized {} from {} to {} bytes: {}",
                request.property,
                report.original.len,
                minimized.len,
                request.out.join(&minimized.dir).display()
            );
            println!("report in {}", report_path.display());
            Ok(())
        }
        (status, _) => Err(format!(
            "{status}: {}; the original is in {}, the report in {}",
            report.reason.as_deref().unwrap_or("no reason given"),
            request.out.join(ORIGINAL_DIR).display(),
            report_path.display()
        )
        .into()),
    }
}

/// The flags `sim-minimize` takes, as given.
fn parse(repo_root: &Path, args: impl Iterator<Item = String>) -> Result<Request, BoxError> {
    let mut positional = Vec::new();
    let mut flags: Vec<(String, String)> = Vec::new();
    let mut args = args;
    while let Some(arg) = args.next() {
        if !arg.starts_with("--") {
            positional.push(arg);
            continue;
        }
        if flags.iter().any(|(flag, _)| *flag == arg) {
            return Err(format!("{arg} given twice").into());
        }
        let value = args.next().ok_or_else(|| format!("{arg} needs a value"))?;
        flags.push((arg, value));
    }
    let [target, input] = <[String; 2]>::try_from(positional)
        .map_err(|_| "`sim-minimize` takes a target and an input")?;
    let mut take = |name: &str| {
        flags
            .iter()
            .position(|(flag, _)| flag == name)
            .map(|index| flags.remove(index).1)
    };
    let need = |value: Option<String>, name: &str| {
        value.ok_or_else(|| format!("`sim-minimize` needs {name}"))
    };
    let secs = |value: String, name: &str| -> Result<Duration, BoxError> {
        let secs: u64 = value.parse().map_err(|error| format!("{name}: {error}"))?;
        if secs == 0 {
            return Err(format!("{name} must be at least 1").into());
        }
        Ok(Duration::from_secs(secs))
    };
    let property = need(take("--property"), "--property")?;
    let out = PathBuf::from(need(take("--out"), "--out")?);
    let runs: u32 = need(take("--runs"), "--runs")?
        .parse()
        .map_err(|error| format!("--runs: {error}"))?;
    if runs == 0 {
        return Err("--runs must be at least 1".into());
    }
    let wall = secs(need(take("--wall-secs"), "--wall-secs")?, "--wall-secs")?;
    let child = secs(need(take("--child-secs"), "--child-secs")?, "--child-secs")?;
    let sanitizer = match take("--sanitizer").as_deref() {
        None | Some("address") => Sanitizer::Address,
        Some("none") => Sanitizer::None,
        Some(other) => return Err(format!("--sanitizer is address or none, not {other}").into()),
    };
    if let Some((flag, _)) = flags.first() {
        return Err(format!("unknown flag `{flag}`").into());
    }
    let harness = resolve_target(repo_root, &target)?;
    Ok(Request {
        target,
        harness: harness.to_owned(),
        input: PathBuf::from(input),
        property,
        out,
        budget: Budget { runs, wall, child },
        sanitizer,
    })
}

/// The harness `target` runs. The target must be in [`TARGETS`] and a
/// `[[bin]]` of the simulation fuzz package's manifest.
fn resolve_target(repo_root: &Path, target: &str) -> Result<&'static str, BoxError> {
    let harness = TARGETS
        .iter()
        .find(|(name, _)| *name == target)
        .map(|(_, harness)| *harness)
        .ok_or_else(|| {
            let known: Vec<_> = TARGETS.iter().map(|(name, _)| *name).collect();
            format!(
                "unknown simulation fuzz target `{target}`; expected one of {}",
                known.join(", ")
            )
        })?;
    let manifest = fs::read_to_string(fuzz_dir(repo_root).join("Cargo.toml"))?;
    let manifest: toml::Value = toml::from_str(&manifest)?;
    let declared = manifest
        .get("bin")
        .and_then(toml::Value::as_array)
        .is_some_and(|bins| {
            bins.iter()
                .any(|bin| bin.get("name").and_then(toml::Value::as_str) == Some(target))
        });
    if !declared {
        return Err(format!("qa/fuzz/sim/Cargo.toml declares no `{target}` target").into());
    }
    Ok(harness)
}

/// The simulation fuzz package.
fn fuzz_dir(repo_root: &Path) -> PathBuf {
    repo_root.join("qa").join("fuzz").join("sim")
}

/// The pinned tools a minimization runs.
pub(super) struct Tools {
    /// The pinned `zakura-sim` binary.
    pub(super) sim: PathBuf,
    /// The pinned fuzz target binary.
    pub(super) fuzz: PathBuf,
    /// The binary `tmin` runs. Its SHA-256 must still match the pinned
    /// target's after `tmin`.
    pub(super) tmin_binary: PathBuf,
    /// The `tmin` command for an input and an artifact prefix directory,
    /// without the filter's variable.
    pub(super) tmin: Box<dyn Fn(&Path, &Path) -> Command>,
    /// The checkout's provenance.
    pub(super) source: Source,
}

/// The checkout the tools were built from.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(super) struct Source {
    /// `HEAD`'s commit.
    pub(super) git: String,
    /// True if `git status` showed any change.
    pub(super) dirty: bool,
}

/// Builds and pins the tools, then minimizes ([`minimize_with`]).
fn minimize(
    repo_root: &Path,
    request: &Request,
    interrupt: &AtomicBool,
) -> Result<Report, BoxError> {
    let started = Instant::now();
    let original = preserve_original(request)?;
    let logs = request.out.join(LOGS_DIR);
    fs::create_dir_all(&logs)?;
    let built = match build_tools(repo_root, request, &logs, interrupt)? {
        Some(tools) => tools,
        None => {
            return write_report(
                request,
                interrupted_before_start(request, original, started),
                started,
            )
        }
    };
    minimize_with(request, &built, original, started, interrupt)
}

/// A report for a run interrupted before its tools were ready.
fn interrupted_before_start(request: &Request, original: Original, started: Instant) -> Report {
    Report::new(request, original, Status::Interrupted, started)
        .with_reason("interrupted while building the tools")
}

/// Builds `zakura-sim` and the fuzz target, and copies both into
/// `<out>/bin/`. `None` if interrupted.
fn build_tools(
    repo_root: &Path,
    request: &Request,
    logs: &Path,
    interrupt: &AtomicBool,
) -> Result<Option<Tools>, BoxError> {
    let cargo = env::var_os("CARGO").unwrap_or_else(|| OsString::from("cargo"));
    let sim_target = repo_root.join("target").join("sim");
    let mut build_sim = Command::new(&cargo);
    build_sim
        .env("RUSTFLAGS", SIM_RUSTFLAGS)
        .env("CARGO_TARGET_DIR", &sim_target)
        .arg("build")
        .arg("--locked")
        .arg("--manifest-path")
        .arg(repo_root.join("Cargo.toml"))
        .args(["-p", "zakura-sim", "--bin", "zakura-sim"]);
    if !build(&mut build_sim, &logs.join("build-sim.log"), interrupt)? {
        return Ok(None);
    }

    let fuzz_target = repo_root.join("target").join("sim-fuzz");
    let fuzz_build = FuzzBuild {
        fuzz_dir: fuzz_dir(repo_root),
        target_dir: fuzz_target.clone(),
        sanitizer: request.sanitizer,
    };
    let mut build_fuzz = fuzz_build.command("build");
    build_fuzz.arg(&request.target);
    if !build(&mut build_fuzz, &logs.join("build-fuzz.log"), interrupt)? {
        return Ok(None);
    }

    let bin = request.out.join(BIN_DIR);
    fs::create_dir_all(&bin)?;
    let sim = bin.join("zakura-sim");
    fs::copy(sim_target.join("debug").join("zakura-sim"), &sim)?;
    let tmin_binary = fuzz_binary(&fuzz_target, &request.target)?;
    let fuzz = bin.join(&request.target);
    fs::copy(&tmin_binary, &fuzz)?;

    let (target, runs) = (request.target.clone(), request.budget.runs);
    let tmin = move |input: &Path, artifacts: &Path| {
        let mut command = fuzz_build.command("tmin");
        command
            .arg("--runs")
            .arg(runs.to_string())
            .arg(&target)
            .arg(input)
            .arg("--")
            .arg(format!("-artifact_prefix={}/", artifacts.display()));
        command
    };
    Ok(Some(Tools {
        sim,
        fuzz,
        tmin_binary,
        tmin: Box::new(tmin),
        source: source(repo_root),
    }))
}

/// The flags every `cargo fuzz` command of one build shares, so `tmin`
/// runs the target `cargo fuzz build` built.
#[derive(Clone)]
struct FuzzBuild {
    /// The simulation fuzz package.
    fuzz_dir: PathBuf,
    /// cargo-fuzz's target directory.
    target_dir: PathBuf,
    /// The sanitizer.
    sanitizer: Sanitizer,
}

impl FuzzBuild {
    /// `cargo +<nightly> fuzz <verb>` with the build's flags.
    fn command(&self, verb: &str) -> Command {
        let mut command = Command::new("cargo");
        command
            .env("RUSTFLAGS", SIM_RUSTFLAGS)
            .arg(format!("+{PINNED_NIGHTLY}"))
            .arg("fuzz")
            .arg(verb)
            .arg("--fuzz-dir")
            .arg(&self.fuzz_dir)
            .arg("--target-dir")
            .arg(&self.target_dir)
            .args(self.sanitizer.flags())
            .current_dir(&self.fuzz_dir);
        command
    }
}

/// Runs a build, logging to `log`. `false` if interrupted.
fn build(command: &mut Command, log: &Path, interrupt: &AtomicBool) -> Result<bool, BoxError> {
    crate::print_command(command);
    match run_child(command, log, None, interrupt)? {
        ChildEnd::Exited(0) => Ok(true),
        ChildEnd::Interrupted => Ok(false),
        end => Err(format!("the build {end}; see {}", log.display()).into()),
    }
}

/// The fuzz target binary cargo-fuzz built under `target_dir`, in its one
/// `<triple>/release/` directory.
fn fuzz_binary(target_dir: &Path, target: &str) -> Result<PathBuf, BoxError> {
    let mut found = Vec::new();
    for entry in fs::read_dir(target_dir)? {
        let path = entry?.path().join("release").join(target);
        if path.is_file() {
            found.push(path);
        }
    }
    match <[PathBuf; 1]>::try_from(found) {
        Ok([path]) => Ok(path),
        Err(found) => Err(format!(
            "expected one built `{target}` under {}, found {}",
            target_dir.display(),
            found.len()
        )
        .into()),
    }
}

/// The checkout's commit and whether it's dirty, as far as git says.
fn source(repo_root: &Path) -> Source {
    let git = |args: &[&str]| {
        Command::new("git")
            .arg("-C")
            .arg(repo_root)
            .args(args)
            .output()
            .ok()
            .filter(|output| output.status.success())
            .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
    };
    Source {
        git: git(&["rev-parse", "HEAD"]).unwrap_or_else(|| "unknown".to_owned()),
        dirty: git(&["status", "--porcelain"]).is_none_or(|status| !status.is_empty()),
    }
}

/// The original input's directory in the output directory.
const ORIGINAL_DIR: &str = "original";

/// The accepted candidate's directory in the output directory.
const MINIMIZED_DIR: &str = "minimized";

/// The candidates' directories in the output directory.
const CANDIDATES_DIR: &str = "candidates";

/// `tmin`'s artifact prefix directory in the output directory.
const TMIN_DIR: &str = "tmin";

/// The pinned binaries in the output directory.
const BIN_DIR: &str = "bin";

/// The build and `tmin` logs in the output directory.
const LOGS_DIR: &str = "logs";

/// An input's bytes within its directory.
const INPUT_FILE: &str = "input.bin";

/// The preserved original.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(super) struct Original {
    /// Its copy, relative to the output directory.
    pub(super) path: PathBuf,
    /// The SHA-256 of the input and of its copy.
    pub(super) sha256: String,
    /// Its length.
    pub(super) len: u64,
    /// The confirmation's result. `None` until it ran.
    pub(super) check: Option<Verdict>,
}

/// Checks that `out` is missing or empty and isn't a symlink, then copies
/// the original into it and checks that the copy's SHA-256 is the
/// input's. Nothing else runs first.
pub(super) fn preserve_original(request: &Request) -> Result<Original, BoxError> {
    let out = &request.out;
    if fs::symlink_metadata(out).is_ok_and(|metadata| metadata.file_type().is_symlink()) {
        return Err(format!("{} is a symlink", out.display()).into());
    }
    fs::create_dir_all(out)?;
    if fs::read_dir(out)?.next().is_some() {
        return Err(format!("{} isn't empty", out.display()).into());
    }
    let bytes = fs::read(&request.input)
        .map_err(|error| format!("reading {}: {error}", request.input.display()))?;
    let dir = out.join(ORIGINAL_DIR);
    fs::create_dir(&dir)?;
    let copy = dir.join(INPUT_FILE);
    fs::write(&copy, &bytes)?;
    let sha256 = sha256(&bytes);
    let copied = sha256_file(&copy)?;
    if copied != sha256 {
        return Err(format!(
            "the copy of {} has SHA-256 {copied}, not {sha256}",
            request.input.display()
        )
        .into());
    }
    Ok(Original {
        path: Path::new(ORIGINAL_DIR).join(INPUT_FILE),
        sha256,
        // A usize always fits in a u64 on supported targets.
        len: bytes.len() as u64,
        check: None,
    })
}

/// How a minimization ended. Each status means one thing.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(super) enum Status {
    /// A smaller candidate passed every check; it's in `minimized/`.
    Minimized,
    /// The original passed every check, and no smaller candidate did.
    OriginalKept,
    /// The original failed a check, so nothing was reduced.
    OriginalRejected,
    /// SIGINT or SIGTERM stopped the run. The original's copy, and any
    /// candidate accepted before, are kept.
    Interrupted,
}

impl fmt::Display for Status {
    /// The status's JSON name.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Status::Minimized => "minimized",
            Status::OriginalKept => "original-kept",
            Status::OriginalRejected => "original-rejected",
            Status::Interrupted => "interrupted",
        })
    }
}

/// How `tmin` ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(super) enum ReductionEnd {
    /// `tmin` didn't run, because the original failed a check or an
    /// interrupt came first.
    NotRun,
    /// `tmin` exited 0: it could reduce no further.
    Finished,
    /// The wrapper stopped `tmin` at `--wall-secs`.
    WallBudget,
    /// `tmin` exited with an error.
    Failed,
    /// An interrupt stopped `tmin`.
    Interrupted,
}

/// What `tmin` did.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(super) struct Reduction {
    /// How it ended.
    pub(super) end: ReductionEnd,
    /// True if `tmin` reduced to its fixed point: [`ReductionEnd::Finished`].
    pub(super) completed: bool,
    /// How `tmin` exited, when it failed.
    pub(super) detail: Option<String>,
    /// How long it ran.
    #[serde(rename = "elapsed_ms", serialize_with = "millis")]
    pub(super) elapsed: Duration,
    /// Its log, relative to the output directory.
    pub(super) log: Option<PathBuf>,
}

/// Why a check rejected an input. Each reason means one thing.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(super) enum Reason {
    /// The fuzz target didn't crash with the property's panic alone.
    WrongCrash,
    /// A check's child ran past `--child-secs` and was killed.
    ChildTimeout,
    /// A check's child died from a signal.
    ChildCrashed,
    /// `zakura-sim confirm` failed without writing a confirmation.
    ChildFailed,
    /// `zakura-sim confirm` wrote a confirmation whose status isn't
    /// `confirmed`; the status is the detail's first word.
    NotConfirmed,
    /// `zakura-sim replay` didn't replay the recorded trace Exact.
    ReplayMismatch,
    /// The fuzz target `tmin` ran isn't the pinned one.
    BuildChanged,
}

/// What a fuzz target's crash was.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(super) enum Crash {
    /// It panicked with the expected property's ID.
    Expected,
    /// It ran the input to its end.
    NoCrash,
    /// libFuzzer reported a timeout.
    Timeout,
    /// libFuzzer reported running out of memory.
    OutOfMemory,
    /// A sanitizer reported an error.
    Sanitizer,
    /// It panicked with another message.
    OtherPanic(String),
    /// It died from a signal without a panic, such as an abort.
    Abort,
    /// It exited in some other way.
    Other(String),
}

impl fmt::Display for Crash {
    /// The crash in words.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Crash::Expected => f.write_str("the expected panic"),
            Crash::NoCrash => f.write_str("no crash"),
            Crash::Timeout => f.write_str("a libFuzzer timeout"),
            Crash::OutOfMemory => f.write_str("libFuzzer's out-of-memory report"),
            Crash::Sanitizer => f.write_str("a sanitizer report"),
            Crash::OtherPanic(message) => write!(f, "a panic with `{message}`"),
            Crash::Abort => f.write_str("a signal without a panic, such as an abort"),
            Crash::Other(how) => write!(f, "an exit with {how}"),
        }
    }
}

/// A check's result for one input.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(super) struct Verdict {
    /// True if every check passed.
    pub(super) accepted: bool,
    /// Why a check rejected the input.
    pub(super) reason: Option<Reason>,
    /// The confirmation's status, when `zakura-sim confirm` wrote one.
    pub(super) confirmation: Option<String>,
    /// The fuzz target's crash, when its check ran.
    pub(super) crash: Option<Crash>,
    /// What the rejection is about.
    pub(super) detail: Option<String>,
    /// The confirmation file, relative to the output directory.
    pub(super) confirmation_file: Option<PathBuf>,
    /// The recorded trace, relative to the output directory.
    pub(super) trace: Option<PathBuf>,
}

impl Verdict {
    /// An accepted input.
    fn accepted(crash: Crash, confirmation_file: PathBuf, trace: PathBuf) -> Self {
        Verdict {
            accepted: true,
            reason: None,
            confirmation: Some("confirmed".to_owned()),
            crash: Some(crash),
            detail: None,
            confirmation_file: Some(confirmation_file),
            trace: Some(trace),
        }
    }

    /// An input rejected for `reason`.
    fn rejected(reason: Reason, detail: impl Into<String>) -> Self {
        Verdict {
            accepted: false,
            reason: Some(reason),
            confirmation: None,
            crash: None,
            detail: Some(detail.into()),
            confirmation_file: None,
            trace: None,
        }
    }
}

/// One `tmin` output smaller than the original.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(super) struct CandidateRecord {
    /// `tmin`'s file, relative to the output directory.
    pub(super) file: PathBuf,
    /// Its SHA-256.
    pub(super) sha256: String,
    /// Its length.
    pub(super) len: u64,
    /// Its checks' result. `None` if the wrapper accepted a smaller
    /// candidate first, or an interrupt came first.
    pub(super) check: Option<Verdict>,
}

/// The candidates' counts.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub(super) struct Counts {
    /// `tmin`'s distinct outputs smaller than the original.
    pub(super) produced: usize,
    /// Those the wrapper checked.
    pub(super) checked: usize,
    /// Those that passed: 0 or 1.
    pub(super) accepted: usize,
    /// Those a check rejected.
    pub(super) rejected: usize,
}

/// The accepted candidate's bundle.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(super) struct Minimized {
    /// The bundle's directory, relative to the output directory: the input,
    /// the logs, the confirmation and the trace.
    pub(super) dir: PathBuf,
    /// Its SHA-256.
    pub(super) sha256: String,
    /// Its length.
    pub(super) len: u64,
    /// Its recorded trace, relative to the output directory.
    pub(super) trace: PathBuf,
}

/// The pinned build.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(super) struct BuildRecord {
    /// The checkout.
    pub(super) source: Source,
    /// The nightly toolchain cargo-fuzz builds with.
    pub(super) toolchain: &'static str,
    /// The fuzz build's sanitizer.
    pub(super) sanitizer: Sanitizer,
    /// The pinned fuzz target's SHA-256.
    pub(super) fuzz_binary: String,
    /// The pinned `zakura-sim` binary's SHA-256.
    pub(super) sim_binary: String,
    /// The simulator build's provenance, from the original's confirmation.
    /// `None` until it ran.
    pub(super) sim_build: Option<Value>,
    /// The harness's generator version, from the same confirmation.
    pub(super) generator: Option<u64>,
}

/// `report.json`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(super) struct Report {
    /// The report's format version.
    pub(super) format: u32,
    /// How the run ended.
    pub(super) status: Status,
    /// Why, unless [`Status::Minimized`].
    pub(super) reason: Option<String>,
    /// The fuzz target.
    pub(super) target: String,
    /// The registered harness it runs.
    pub(super) harness: String,
    /// The property.
    pub(super) property: String,
    /// The limits.
    pub(super) budget: Budget,
    /// The pinned build. `None` if the tools weren't built.
    pub(super) build: Option<BuildRecord>,
    /// The original.
    pub(super) original: Original,
    /// What `tmin` did.
    pub(super) reduction: Reduction,
    /// The candidates' counts.
    pub(super) counts: Counts,
    /// Every candidate, smallest first.
    pub(super) candidates: Vec<CandidateRecord>,
    /// The accepted candidate.
    pub(super) minimized: Option<Minimized>,
    /// How long the whole run took.
    #[serde(rename = "elapsed_ms", serialize_with = "millis")]
    pub(super) elapsed: Duration,
}

impl Report {
    /// A report with no reduction or candidates yet.
    fn new(request: &Request, original: Original, status: Status, started: Instant) -> Self {
        Report {
            format: REPORT_FORMAT,
            status,
            reason: None,
            target: request.target.clone(),
            harness: request.harness.clone(),
            property: request.property.clone(),
            budget: request.budget,
            build: None,
            original,
            reduction: Reduction {
                end: ReductionEnd::NotRun,
                completed: false,
                detail: None,
                elapsed: Duration::ZERO,
                log: None,
            },
            counts: Counts::default(),
            candidates: Vec::new(),
            minimized: None,
            elapsed: started.elapsed(),
        }
    }

    /// The report with `reason`.
    fn with_reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = Some(reason.into());
        self
    }
}

/// Confirms the original, runs `tmin`, and confirms its candidates, with
/// `tools`. Writes and returns the report. The original must already be
/// preserved ([`preserve_original`]).
pub(super) fn minimize_with(
    request: &Request,
    tools: &Tools,
    original: Original,
    started: Instant,
    interrupt: &AtomicBool,
) -> Result<Report, BoxError> {
    let out = &request.out;
    let mut report = Report::new(request, original, Status::OriginalKept, started);
    report.build = Some(BuildRecord {
        source: tools.source.clone(),
        toolchain: PINNED_NIGHTLY,
        sanitizer: request.sanitizer,
        fuzz_binary: sha256_file(&tools.fuzz)?,
        sim_binary: sha256_file(&tools.sim)?,
        sim_build: None,
        generator: None,
    });

    // The original, in fresh processes.
    let checker = Checker {
        request,
        tools,
        interrupt,
    };
    let original_dir = out.join(ORIGINAL_DIR);
    let Some(verdict) = checker.check_input(&original_dir, None)? else {
        report.status = Status::Interrupted;
        return write_report(
            request,
            report.with_reason("interrupted while checking the original"),
            started,
        );
    };
    if let (Some(build), Some(file)) = (report.build.as_mut(), &verdict.confirmation_file) {
        let confirmation = read_json(&out.join(file))?;
        build.sim_build = confirmation.get("build").cloned();
        build.generator = confirmation.get("generator").and_then(Value::as_u64);
    }
    let accepted = verdict.accepted;
    let detail = describe(&verdict);
    report.original.check = Some(verdict);
    if !accepted {
        report.status = Status::OriginalRejected;
        return write_report(request, report.with_reason(detail), started);
    }

    // The reduction.
    let tmin_dir = out.join(TMIN_DIR);
    fs::create_dir_all(&tmin_dir)?;
    let log = Path::new(LOGS_DIR).join("tmin.log");
    let mut command = (tools.tmin)(&out.join(&report.original.path), &tmin_dir);
    command.env(EXPECT_VAR, &request.property).env(
        "ZAKURA_SIM_ARTIFACTS",
        out.join(LOGS_DIR).join("sim-artifacts"),
    );
    crate::print_command(&command);
    let tmin_started = Instant::now();
    let end = run_child(
        &mut command,
        &out.join(&log),
        Some(request.budget.wall),
        interrupt,
    )?;
    report.reduction = Reduction {
        end: match end {
            ChildEnd::Exited(0) => ReductionEnd::Finished,
            ChildEnd::TimedOut => ReductionEnd::WallBudget,
            ChildEnd::Interrupted => ReductionEnd::Interrupted,
            ChildEnd::Exited(_) | ChildEnd::Signaled(_) => ReductionEnd::Failed,
        },
        completed: end == ChildEnd::Exited(0),
        detail: match end {
            ChildEnd::Exited(code) if code != 0 => Some(format!("tmin {end}")),
            ChildEnd::Signaled(_) => Some(format!("tmin {end}")),
            _ => None,
        },
        elapsed: tmin_started.elapsed(),
        log: Some(log),
    };
    if end == ChildEnd::Interrupted {
        report.status = Status::Interrupted;
        report.candidates = candidates(out, &tmin_dir, &report.original)?;
        report.counts.produced = report.candidates.len();
        return write_report(
            request,
            report.with_reason("interrupted while tmin ran"),
            started,
        );
    }

    // The candidates, smallest first.
    report.candidates = candidates(out, &tmin_dir, &report.original)?;
    report.counts.produced = report.candidates.len();
    let rebuilt = sha256_file(&tools.tmin_binary)?;
    let pinned = report
        .build
        .as_ref()
        .map(|build| build.fuzz_binary.clone())
        .unwrap_or_default();
    let against = report
        .original
        .check
        .as_ref()
        .and_then(|check| check.confirmation_file.clone())
        .map(|file| out.join(file));
    for index in 0..report.candidates.len() {
        if interrupt.load(Ordering::SeqCst) {
            report.status = Status::Interrupted;
            return write_report(
                request,
                report.with_reason("interrupted while checking a candidate"),
                started,
            );
        }
        let candidate = &report.candidates[index];
        let dir = out
            .join(CANDIDATES_DIR)
            .join(format!("{index:03}-{}", &candidate.sha256[..12]));
        fs::create_dir_all(&dir)?;
        fs::copy(out.join(&candidate.file), dir.join(INPUT_FILE))?;
        let verdict = if rebuilt != pinned {
            Some(Verdict::rejected(
                Reason::BuildChanged,
                format!("tmin ran a fuzz target with SHA-256 {rebuilt}, not the pinned {pinned}"),
            ))
        } else {
            checker.check_input(&dir, against.as_deref())?
        };
        let Some(verdict) = verdict else {
            report.status = Status::Interrupted;
            return write_report(
                request,
                report.with_reason("interrupted while checking a candidate"),
                started,
            );
        };
        report.counts.checked += 1;
        let accepted = verdict.accepted;
        report.candidates[index].check = Some(verdict);
        if !accepted {
            report.counts.rejected += 1;
            continue;
        }
        report.counts.accepted += 1;
        let bundle = out.join(MINIMIZED_DIR);
        fs::rename(&dir, &bundle)?;
        let candidate = &mut report.candidates[index];
        let check = candidate
            .check
            .as_mut()
            .expect("the candidate's check was just stored");
        rebase(check, &dir, &bundle, out);
        let trace = check
            .trace
            .clone()
            .expect("an accepted verdict names its trace");
        report.minimized = Some(Minimized {
            dir: PathBuf::from(MINIMIZED_DIR),
            sha256: candidate.sha256.clone(),
            len: candidate.len,
            trace,
        });
        report.status = Status::Minimized;
        return write_report(request, report, started);
    }
    let reason = if report.candidates.is_empty() {
        "tmin produced no candidate smaller than the original"
    } else {
        "every candidate smaller than the original was rejected"
    };
    write_report(request, report.with_reason(reason), started)
}

/// Moves `check`'s paths from `from` to `to`, both under `out`.
fn rebase(check: &mut Verdict, from: &Path, to: &Path, out: &Path) {
    let (Ok(from), Ok(to)) = (from.strip_prefix(out), to.strip_prefix(out)) else {
        return;
    };
    for path in [&mut check.confirmation_file, &mut check.trace]
        .into_iter()
        .flatten()
    {
        if let Ok(rest) = path.strip_prefix(from) {
            *path = to.join(rest);
        }
    }
}

/// A verdict in words, for the report's reason.
fn describe(verdict: &Verdict) -> String {
    let reason = match &verdict.reason {
        Some(reason) => serde_json::to_value(reason)
            .ok()
            .and_then(|value| value.as_str().map(str::to_owned))
            .unwrap_or_default(),
        None => "accepted".to_owned(),
    };
    match &verdict.detail {
        Some(detail) => format!("{reason}: {detail}"),
        None => reason,
    }
}

/// Every distinct file in `tmin_dir` smaller than the original, smallest
/// first, then by name.
fn candidates(
    out: &Path,
    tmin_dir: &Path,
    original: &Original,
) -> Result<Vec<CandidateRecord>, BoxError> {
    let mut files = Vec::new();
    for entry in fs::read_dir(tmin_dir)? {
        let path = entry?.path();
        if !path.is_file() {
            continue;
        }
        let bytes = fs::read(&path)?;
        // A usize always fits in a u64 on supported targets.
        let len = bytes.len() as u64;
        if len >= original.len {
            continue;
        }
        let file = path
            .strip_prefix(out)
            .map_or_else(|_| path.clone(), Path::to_path_buf);
        files.push(CandidateRecord {
            file,
            sha256: sha256(&bytes),
            len,
            check: None,
        });
    }
    files.sort_by(|a, b| (a.len, &a.file).cmp(&(b.len, &b.file)));
    let mut seen = std::collections::BTreeSet::new();
    files.retain(|file| seen.insert(file.sha256.clone()));
    Ok(files)
}

/// Writes `report` to `<out>/report.json` through a temporary file, so a
/// reader never sees half of it, and returns it. Its elapsed time counts
/// from `started`.
fn write_report(
    request: &Request,
    mut report: Report,
    started: Instant,
) -> Result<Report, BoxError> {
    report.elapsed = started.elapsed();
    let path = request.out.join(REPORT_FILE);
    let partial = request.out.join(format!(".{REPORT_FILE}.partial"));
    fs::write(
        &partial,
        format!("{}\n", serde_json::to_string_pretty(&report)?),
    )?;
    fs::rename(&partial, &path)?;
    Ok(report)
}

/// Runs the three fresh-process checks of one input.
struct Checker<'a> {
    request: &'a Request,
    tools: &'a Tools,
    interrupt: &'a AtomicBool,
}

impl Checker<'_> {
    /// Checks `dir/input.bin` in three fresh processes, logging into `dir`:
    ///
    /// 1. the pinned fuzz target, with the filter set, must crash with the
    ///    property's panic alone ([`classify_crash`]);
    /// 2. `zakura-sim confirm` must confirm the property, against the
    ///    original's confirmation `against` if given;
    /// 3. `zakura-sim replay` must replay the confirmed trace Exact.
    ///
    /// The first failed check rejects the input. `None` if interrupted.
    fn check_input(&self, dir: &Path, against: Option<&Path>) -> Result<Option<Verdict>, BoxError> {
        let out = &self.request.out;
        let input = dir.join(INPUT_FILE);
        let child = self.request.budget.child;
        let relative = |path: &Path| {
            path.strip_prefix(out)
                .map_or_else(|_| path.to_path_buf(), Path::to_path_buf)
        };

        let fuzz_log = dir.join("fuzz.log");
        let crashes = dir.join("fuzz-artifacts");
        fs::create_dir_all(&crashes)?;
        let mut fuzz = Command::new(&self.tools.fuzz);
        fuzz.env(EXPECT_VAR, &self.request.property)
            .env("ZAKURA_SIM_ARTIFACTS", dir.join("sim-artifacts"))
            .arg(format!("-timeout={}", child.as_secs().max(1)))
            .arg(format!("-artifact_prefix={}/", crashes.display()))
            .arg(&input);
        let crash = match run_child(
            &mut fuzz,
            &fuzz_log,
            Some(child + FUZZ_GRACE),
            self.interrupt,
        )? {
            ChildEnd::Interrupted => return Ok(None),
            ChildEnd::TimedOut => {
                return Ok(Some(Verdict::rejected(
                    Reason::ChildTimeout,
                    format!(
                        "the fuzz target ran past {} s",
                        (child + FUZZ_GRACE).as_secs()
                    ),
                )))
            }
            end => classify_crash(end, &read_log(&fuzz_log)?, &self.request.property),
        };
        if crash != Crash::Expected {
            let mut verdict = Verdict::rejected(
                Reason::WrongCrash,
                format!("the fuzz target ended with {crash}"),
            );
            verdict.crash = Some(crash);
            return Ok(Some(verdict));
        }

        let confirm_dir = dir.join("confirm");
        let confirm_log = dir.join("confirm.log");
        let mut confirm = Command::new(&self.tools.sim);
        confirm
            .arg("confirm")
            .arg("--sim")
            .arg(&self.request.harness)
            .arg("--input")
            .arg(&input)
            .arg("--property")
            .arg(&self.request.property)
            .arg("--out")
            .arg(&confirm_dir);
        if let Some(against) = against {
            confirm.arg("--against").arg(against);
        }
        let confirmation_file = confirm_dir.join(CONFIRMATION_FILE);
        let ended = run_child(&mut confirm, &confirm_log, Some(child), self.interrupt)?;
        let rejected = |reason, detail: String| {
            let mut verdict = Verdict::rejected(reason, detail);
            verdict.crash = Some(Crash::Expected);
            Ok(Some(verdict))
        };
        match ended {
            ChildEnd::Interrupted => return Ok(None),
            ChildEnd::TimedOut => {
                return rejected(
                    Reason::ChildTimeout,
                    format!("zakura-sim confirm ran past {} s", child.as_secs()),
                )
            }
            ChildEnd::Signaled(_) => {
                return rejected(Reason::ChildCrashed, format!("zakura-sim confirm {ended}"))
            }
            ChildEnd::Exited(2) => {
                return Err(format!(
                    "zakura-sim confirm refused its arguments; see {}",
                    confirm_log.display()
                )
                .into())
            }
            ChildEnd::Exited(_) => {}
        }
        let confirmation = match fs::read(&confirmation_file) {
            Ok(bytes) => serde_json::from_slice::<Value>(&bytes).ok(),
            Err(_) => None,
        };
        let Some(confirmation) = confirmation else {
            return rejected(
                Reason::ChildFailed,
                format!(
                    "zakura-sim confirm {ended} without a confirmation: {}",
                    tail(&read_log(&confirm_log)?)
                ),
            );
        };
        let status = confirmation
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("missing")
            .to_owned();
        if status != "confirmed" || ended != ChildEnd::Exited(0) {
            let detail = confirmation
                .get("detail")
                .and_then(Value::as_str)
                .map_or_else(String::new, |detail| format!(": {detail}"));
            let mut verdict = Verdict::rejected(Reason::NotConfirmed, format!("{status}{detail}"));
            verdict.crash = Some(Crash::Expected);
            verdict.confirmation = Some(status);
            verdict.confirmation_file = Some(relative(&confirmation_file));
            return Ok(Some(verdict));
        }
        let Some(trace) = confirmation.get("trace").and_then(Value::as_str) else {
            return rejected(
                Reason::ChildFailed,
                "the confirmation names no trace".to_owned(),
            );
        };
        let trace = confirm_dir.join(trace);

        let replay_log = dir.join("replay.log");
        let mut replay = Command::new(&self.tools.sim);
        replay.arg("replay").arg(&trace);
        let replayed = run_child(&mut replay, &replay_log, Some(child), self.interrupt)?;
        let rejection = match replayed {
            ChildEnd::Interrupted => return Ok(None),
            ChildEnd::Exited(0) => None,
            ChildEnd::TimedOut => Some((
                Reason::ChildTimeout,
                format!("zakura-sim replay ran past {} s", child.as_secs()),
            )),
            ChildEnd::Signaled(_) => Some((
                Reason::ChildCrashed,
                format!("zakura-sim replay {replayed}"),
            )),
            ChildEnd::Exited(_) => Some((
                Reason::ReplayMismatch,
                format!(
                    "zakura-sim replay {replayed}: {}",
                    tail(&read_log(&replay_log)?)
                ),
            )),
        };
        let mut verdict = match rejection {
            None => Verdict::accepted(
                Crash::Expected,
                relative(&confirmation_file),
                relative(&trace),
            ),
            Some((reason, detail)) => {
                let mut verdict = Verdict::rejected(reason, detail);
                verdict.crash = Some(Crash::Expected);
                verdict.confirmation_file = Some(relative(&confirmation_file));
                verdict.trace = Some(relative(&trace));
                verdict
            }
        };
        verdict.confirmation = Some(status);
        Ok(Some(verdict))
    }
}

/// The crash the fuzz target's run `end` with output `log` shows, for the
/// expected `property`.
///
/// libFuzzer's own reports come first: a timeout, running out of memory, or
/// a sanitizer's error. Then the last panic message: the filter panics with
/// the property's ID alone, and the abort that follows is libFuzzer's
/// "deadly signal". A task panic the scheduler caught prints a message
/// too, but an expected crash's panic is always the last.
pub(super) fn classify_crash(end: ChildEnd, log: &str, property: &str) -> Crash {
    if end == ChildEnd::Exited(0) {
        return Crash::NoCrash;
    }
    if log.contains("ERROR: libFuzzer: timeout") {
        return Crash::Timeout;
    }
    if log.contains("ERROR: libFuzzer: out-of-memory") {
        return Crash::OutOfMemory;
    }
    if log
        .lines()
        .any(|line| line.contains("ERROR: ") && line.contains("Sanitizer"))
    {
        return Crash::Sanitizer;
    }
    let mut lines = log.lines();
    let mut last_panic = None;
    while let Some(line) = lines.next() {
        if line.starts_with("thread '") && line.contains("' panicked at ") {
            last_panic = lines.next().map(str::to_owned);
        }
    }
    let deadly = log.contains("ERROR: libFuzzer: deadly signal");
    match last_panic {
        Some(message) if message == property && deadly => Crash::Expected,
        Some(message) => Crash::OtherPanic(message),
        None if deadly || matches!(end, ChildEnd::Signaled(_)) => Crash::Abort,
        None => Crash::Other(end.to_string()),
    }
}

/// How a child ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ChildEnd {
    /// It exited with this code.
    Exited(i32),
    /// A signal killed it.
    Signaled(i32),
    /// It ran past its deadline, and the wrapper killed its process group.
    TimedOut,
    /// The interrupt flag was set, and the wrapper killed its process group.
    Interrupted,
}

impl fmt::Display for ChildEnd {
    /// The end in words.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ChildEnd::Exited(code) => write!(f, "exited with code {code}"),
            ChildEnd::Signaled(signal) => write!(f, "died from signal {signal}"),
            ChildEnd::TimedOut => f.write_str("ran past its deadline"),
            ChildEnd::Interrupted => f.write_str("was interrupted"),
        }
    }
}

/// Runs `command` in a process group of its own, with stdin closed and its
/// stdout and stderr in `log`. It's killed with its whole group when
/// `deadline` passes or `interrupt` is set.
pub(super) fn run_child(
    command: &mut Command,
    log: &Path,
    deadline: Option<Duration>,
    interrupt: &AtomicBool,
) -> Result<ChildEnd, BoxError> {
    if let Some(parent) = log.parent() {
        fs::create_dir_all(parent)?;
    }
    let file = fs::File::create(log)?;
    command
        .stdin(Stdio::null())
        .stdout(file.try_clone()?)
        .stderr(file);
    own_process_group(command);
    let mut child = command
        .spawn()
        .map_err(|error| format!("starting {:?}: {error}", command.get_program()))?;
    let started = Instant::now();
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(exit(status));
        }
        let end = if interrupt.load(Ordering::SeqCst) {
            ChildEnd::Interrupted
        } else if deadline.is_some_and(|deadline| started.elapsed() >= deadline) {
            ChildEnd::TimedOut
        } else {
            thread::sleep(POLL);
            continue;
        };
        kill_group(&mut child);
        child.wait()?;
        return Ok(end);
    }
}

/// `status` as a [`ChildEnd`].
fn exit(status: ExitStatus) -> ChildEnd {
    match status.code() {
        Some(code) => ChildEnd::Exited(code),
        None => ChildEnd::Signaled(signal(status)),
    }
}

/// Puts the child in a process group of its own, so the wrapper can kill
/// everything it starts, such as `cargo fuzz tmin`'s fuzz target.
#[cfg(unix)]
fn own_process_group(command: &mut Command) {
    use std::os::unix::process::CommandExt as _;
    command.process_group(0);
}

/// Without process groups, only the child itself can be killed.
#[cfg(not(unix))]
fn own_process_group(_: &mut Command) {}

/// Kills the child's process group, which [`own_process_group`] made.
#[cfg(unix)]
fn kill_group(child: &mut std::process::Child) {
    if let Ok(pid) = libc::pid_t::try_from(child.id()) {
        // SAFETY: `kill` has no memory-safety preconditions. The group is
        // the child's own, so the signal reaches nothing else.
        unsafe {
            libc::kill(-pid, libc::SIGKILL);
        }
    }
    let _ = child.kill();
}

/// Kills the child.
#[cfg(not(unix))]
fn kill_group(child: &mut std::process::Child) {
    let _ = child.kill();
}

/// The signal that killed a child.
#[cfg(unix)]
fn signal(status: ExitStatus) -> i32 {
    use std::os::unix::process::ExitStatusExt as _;
    status.signal().unwrap_or_default()
}

/// No signals outside Unix.
#[cfg(not(unix))]
fn signal(_: ExitStatus) -> i32 {
    0
}

/// Sets [`INTERRUPTED`] on SIGINT and SIGTERM. Children run in their own
/// process groups, so a terminal's Ctrl-C reaches only the wrapper, which
/// then kills the running child and writes the report.
#[cfg(unix)]
fn install_interrupt_handler() {
    extern "C" fn interrupted(_: libc::c_int) {
        INTERRUPTED.store(true, Ordering::SeqCst);
    }
    let handler = interrupted as extern "C" fn(libc::c_int);
    for signal in [libc::SIGINT, libc::SIGTERM] {
        // SAFETY: the handler only stores to an atomic, which is
        // async-signal-safe.
        unsafe {
            libc::signal(signal, handler as libc::sighandler_t);
        }
    }
}

/// Outside Unix, an interrupt kills the wrapper as usual.
#[cfg(not(unix))]
fn install_interrupt_handler() {}

/// A log's text, lossily, at most its last 64 KiB.
fn read_log(path: &Path) -> io::Result<String> {
    const LIMIT: u64 = 64 << 10;
    let mut file = fs::File::open(path)?;
    let len = file.metadata()?.len();
    if len > LIMIT {
        use std::io::{Seek as _, SeekFrom};
        file.seek(SeekFrom::Start(len - LIMIT))?;
    }
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}

/// The last non-empty line of `log`.
fn tail(log: &str) -> String {
    log.lines()
        .rev()
        .find(|line| !line.trim().is_empty())
        .unwrap_or("no output")
        .to_owned()
}

/// A JSON file.
fn read_json(path: &Path) -> Result<Value, BoxError> {
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}

/// `bytes`' SHA-256 in lowercase hex.
fn sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// A file's SHA-256 in lowercase hex.
fn sha256_file(path: &Path) -> Result<String, BoxError> {
    Ok(sha256(&fs::read(path).map_err(|error| {
        format!("reading {}: {error}", path.display())
    })?))
}

/// Serializes a duration as whole milliseconds.
fn millis<S: serde::Serializer>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_u64(u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
}

#[cfg(all(test, unix))]
mod tests;
