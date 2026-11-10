//! `cargo xtask planted loop`: the mutation loop's runner (mutation plan,
//! mut-7.3).
//!
//! It reads the loop's cases: the entries of
//! `qa/native-mutations/cases.json` that name a `property`, the cases of a
//! generated directory's own `cases.json` with `--cases <dir>`, or, with
//! `--planted <prefix>`, the retained `qa/planted/` cases whose path under
//! `qa/planted/` starts with the prefix. A property ID prefix selects its
//! cases, and a case's path without `.patch` selects that case alone. Each case names a patch, the property the patch
//! must violate, and one witness: a planted case's committed input or
//! typed scenario, a committed fuzz input, or one `zakura-sim` test.
//!
//! The runner checks out `HEAD`, the pinned revision, once per worker slot
//! under `--work`. Each slot owns its worktree and target directory, and
//! runs one mutant at a time, resetting the worktree to the pin before
//! each. Every build uses the `planted` profile and the simulator's flag.
//! Then:
//!
//! 1. Slot 0 builds the unpatched witnesses, and the runner runs each
//!    witness once. A witness whose run doesn't hold its case's property
//!    cleanly fails its baseline, and no mutant runs against it for that
//!    property.
//! 2. Each slot takes the next mutant, applies its patch and rebuilds. Then
//!    it runs each of the mutant's cases' witnesses. A shared-path patch
//!    has one case per protocol table, so each table is classified on its
//!    own.
//! 3. It writes `report.json` to `--out`, which must be missing or empty.
//!
//! Each witness runs with `ZAKURA_SIM_END_RECORDS` naming a fresh
//! directory, so `zakura-sim` writes each harness run's end record and
//! each witness judgement there. The runner classifies the run from those
//! records ([`classify`]), and never reads the witness's output. Each
//! record names its build's source digest, and only records whose digest
//! is the patched tree's count.
//!
//! Each witness process runs under `systemd-run --scope -p MemoryMax=<cap>`.
//! The cap is the host's memory divided by the slots, unless
//! `--memory-max` sets it. A baseline witness or a case the cap killed
//! reruns alone, with the host's memory as its cap. A mutant's deadline is
//! five times its baseline's time, and at least 60 s. A timeout is a
//! harness failure, never a kill or a survivor.
//!
//! `--changed-since <rev>` keeps only the cases a change since `rev` can
//! affect, by `cargo xtask planted`'s rule: the patch, the case file, the
//! witness's files, the files the patch changes, and the shared machinery.
//!
//! The loop runs only on a remote host. No workflow runs it.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    env, fs, io,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{atomic::Ordering, Mutex},
    thread,
    time::{Duration, Instant, SystemTime},
};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::{
    apply, changed_since_rev, git, is_affected, patched_files,
    pilot::{
        add_worktree, file_sha256, fresh_dir, head, hex, sim_build_failure, BuildFailure,
        INTERRUPTED_ERROR,
    },
    planted_layout, provenance, remove_worktree, CaseFile, TypedFile, PLANTED_DIR, PROFILE, SHARED,
};
use crate::{
    run_command,
    sim::SIM_RUSTFLAGS,
    sim_minimize::{install_interrupt_handler, ChildEnd, INTERRUPTED},
    BoxError, UsageError,
};

mod classify;
#[cfg(test)]
mod tests;

use classify::{Class, Classified, Ended, Observed, Reason, Trigger};

/// The loop's retained cases, relative to the repository root.
const CASES: &str = "qa/native-mutations/cases.json";

/// The generator prompt, whose hash the report records.
const PROMPT: &str = "qa/native-mutations/PROMPT.md";

/// The report's file name under `--out`.
const REPORT_FILE: &str = "report.json";

/// The report's schema version.
const SCHEMA: u32 = 1;

/// The variable that names `zakura-sim`'s end-record directory. It is
/// `zakura_sim::trace::end_records::END_RECORDS_VAR`, which xtask can't
/// import, since the simulator crate is empty outside the simulator build.
const RECORDS_VAR: &str = "ZAKURA_SIM_END_RECORDS";

/// The shortest deadline a mutant's witness gets.
const MIN_DEADLINE: Duration = Duration::from_secs(60);

/// A mutant's deadline, as a multiple of its baseline's time.
const DEADLINE_FACTOR: u32 = 5;

/// The longest a baseline witness may run: `qa/suite.toml`'s
/// `planted-timeout-secs`.
const BASELINE_DEADLINE: Duration = Duration::from_secs(3600);

/// The slots by default.
const DEFAULT_JOBS: usize = 4;

/// How often a running child is polled.
const POLL: Duration = Duration::from_millis(50);

/// A case's witness, as `cases.json` names it.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(untagged)]
pub(super) enum WitnessSpec {
    /// A retained planted case's committed input or typed scenario.
    Planted {
        /// The case file, relative to the repository root.
        planted: PathBuf,
    },
    /// A committed fuzz input, whose sidecar names its harness.
    Input {
        /// The input, relative to the repository root.
        input: PathBuf,
    },
    /// One `zakura-sim` integration test.
    Test {
        /// The test's name, which `--exact` selects.
        test: String,
        /// The test file's name under `crates/zakura-sim/tests/`.
        test_target: String,
    },
}

/// One loop entry of a `cases.json`.
#[derive(Clone, Debug, Deserialize)]
struct CaseEntry {
    patch: PathBuf,
    property: String,
    witness: WitnessSpec,
    #[serde(default)]
    entry: Option<String>,
    #[serde(default)]
    table: Option<String>,
    #[serde(default)]
    also_violates: Vec<String>,
}

/// One case the loop runs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct LoopCase {
    /// The patch's path, as the report names it.
    pub(super) label: String,
    /// The patch, absolute.
    pub(super) patch: PathBuf,
    /// The case file that lists it, absolute.
    pub(super) listed_in: PathBuf,
    pub(super) property: String,
    pub(super) witness: WitnessSpec,
    pub(super) entry: Option<String>,
    pub(super) table: Option<String>,
    pub(super) also_violates: Vec<String>,
}

/// A witness as the runner runs it.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Runnable {
    /// `zakura-sim record` on a harness, with its input's arguments, as a
    /// witness test of `property` that expects it to hold.
    Record {
        harness: String,
        args: Vec<String>,
        property: String,
    },
    /// One test in one test binary.
    Test { target: String, test: String },
}

/// What a report records about one case.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct CaseReport {
    pub(super) patch: String,
    pub(super) patch_sha256: String,
    pub(super) property: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) entry: Option<String>,
    /// The protocol table, for a shared-path case.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) table: Option<String>,
    pub(super) witness: WitnessSpec,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(super) also_violates: Vec<String>,
    pub(super) result: Classified,
    /// Whether the run that decided the result reached the named ID's
    /// trigger history ([`classify::trigger`]), if the witness ran.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) trigger: Option<Trigger>,
    pub(super) baseline: BaselineReport,
    /// How long the mutant's witness ran, if it ran.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) millis: Option<u64>,
    /// The witness process's peak resident set, if it ran.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) peak_rss_kib: Option<u64>,
    /// The witness's log and records, relative to `--out`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) log: Option<PathBuf>,
    /// True if the memory cap killed the first run, and this result is the
    /// rerun alone with the host's memory.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub(super) rerun_alone: bool,
}

/// The unpatched witness's run for a case's property.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct BaselineReport {
    pub(super) passed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) detail: Option<String>,
    pub(super) millis: u64,
    /// True if the memory cap killed the first run, and this result is the
    /// rerun alone with the host's memory.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub(super) rerun_alone: bool,
}

/// The build every case ran on, unpatched.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct BuildIdentity {
    /// The unpatched tree's source digest, as `zakura-sim`'s build script
    /// computes it.
    pub(super) source: String,
    pub(super) rustc: String,
    pub(super) profile: String,
    pub(super) rustflags: String,
}

/// One property's cases, counted by class: mutant results apart from run
/// failures. The report computes no score.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct PropertySummary {
    pub(super) results: BTreeMap<String, usize>,
    pub(super) run_failures: BTreeMap<String, usize>,
}

/// One run's report.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct Report {
    pub(super) schema: u32,
    /// The pinned revision every case built on.
    pub(super) pinned: String,
    pub(super) prompt_sha256: String,
    pub(super) build: BuildIdentity,
    pub(super) host: String,
    pub(super) jobs: usize,
    /// Each witness process's memory cap, in bytes, if any.
    pub(super) memory_max: Option<u64>,
    pub(super) cases: Vec<CaseReport>,
    pub(super) summary: BTreeMap<String, PropertySummary>,
}

/// The per-property summary of `cases`.
pub(super) fn summarize(cases: &[CaseReport]) -> BTreeMap<String, PropertySummary> {
    let mut summary: BTreeMap<String, PropertySummary> = BTreeMap::new();
    for case in cases {
        let counts = summary.entry(case.property.clone()).or_default();
        let counts = if case.result.class.is_result() {
            &mut counts.results
        } else {
            &mut counts.run_failures
        };
        *counts.entry(case.result.key()).or_default() += 1;
    }
    summary
}

/// The flags after `planted loop`.
#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct Flags {
    pub(super) out: PathBuf,
    pub(super) work: Option<PathBuf>,
    pub(super) cases: Option<PathBuf>,
    pub(super) planted: Vec<String>,
    pub(super) only: Vec<String>,
    pub(super) changed_since: Option<String>,
    pub(super) jobs: usize,
    /// `Some(None)` for `--memory-max off`.
    pub(super) memory_max: Option<Option<u64>>,
}

/// Reads the flags after `planted loop`. `--out` and `--work` become
/// absolute, since cargo runs from each slot's worktree.
pub(super) fn parse_flags(mut args: impl Iterator<Item = String>) -> Result<Flags, BoxError> {
    let mut flags = Flags {
        jobs: DEFAULT_JOBS,
        ..Flags::default()
    };
    let mut out = None;
    while let Some(flag) = args.next() {
        let value = args.next().ok_or_else(|| format!("{flag} needs a value"))?;
        match flag.as_str() {
            "--out" if out.is_none() => out = Some(std::path::absolute(value)?),
            "--work" if flags.work.is_none() => flags.work = Some(std::path::absolute(value)?),
            "--cases" if flags.cases.is_none() => flags.cases = Some(std::path::absolute(value)?),
            "--planted" => flags.planted.push(value),
            "--only" => flags.only.push(value),
            "--changed-since" if flags.changed_since.is_none() => flags.changed_since = Some(value),
            "--jobs" => {
                flags.jobs = value
                    .parse()
                    .ok()
                    .filter(|jobs| *jobs > 0)
                    .ok_or_else(|| format!("--jobs takes a positive count, not `{value}`"))?;
            }
            "--memory-max" if flags.memory_max.is_none() => {
                flags.memory_max = Some(match value.as_str() {
                    "off" => None,
                    bytes => Some(bytes.parse().map_err(|_| {
                        format!("--memory-max takes a byte count or `off`, not `{bytes}`")
                    })?),
                });
            }
            _ => return Err(format!("unknown or repeated flag `{flag}`").into()),
        }
    }
    flags.out = out.ok_or(UsageError("`planted loop` needs --out <dir>"))?;
    Ok(flags)
}

/// Runs `cargo xtask planted loop`.
pub(crate) fn run(repo_root: &Path, args: impl Iterator<Item = String>) -> Result<(), BoxError> {
    let flags = parse_flags(args)?;
    install_interrupt_handler();
    let mut cases = Vec::new();
    if flags.cases.is_some() || flags.planted.is_empty() {
        let dir = flags.cases.clone().unwrap_or_else(|| {
            repo_root
                .join(CASES)
                .parent()
                .map(Path::to_path_buf)
                .unwrap_or_default()
        });
        cases.extend(native_cases(repo_root, &dir)?);
    }
    cases.extend(planted_cases(repo_root, &flags.planted)?);
    let cases = select(
        repo_root,
        cases,
        &flags.only,
        flags.changed_since.as_deref(),
    )?;
    let Some(cases) = cases else {
        println!("no loop case is affected by the changes since the revision");
        return Ok(());
    };
    fresh_dir(&flags.out)?;
    let work = flags
        .work
        .clone()
        .unwrap_or_else(|| repo_root.join("target").join("mutation-loop"));
    let memory_max = match flags.memory_max {
        Some(cap) => cap,
        None => Some(host_memory()? / flags.jobs as u64),
    };
    let pinned = head(repo_root)?;
    let runner = Runner {
        out: flags.out.clone(),
        work,
        pinned: pinned.clone(),
        memory_max,
        build_jobs: (thread::available_parallelism().map_or(1, usize::from) / flags.jobs).max(1),
    };
    let slots: Vec<Slot> = (0..flags.jobs).map(|index| runner.slot(index)).collect();
    for slot in &slots {
        add_worktree(repo_root, &slot.tree, &pinned)?;
    }
    let outcome = runner.run_all(&cases, &slots);
    for slot in &slots {
        let _ = remove_worktree(repo_root, &slot.tree);
        let _ = fs::remove_dir_all(&slot.target);
    }
    let (source, results) = outcome?;
    let report = Report {
        schema: SCHEMA,
        pinned,
        prompt_sha256: file_sha256(&repo_root.join(PROMPT)).unwrap_or_default(),
        build: BuildIdentity {
            source,
            rustc: command_line(Command::new("rustc").arg("--version")),
            profile: PROFILE.to_owned(),
            rustflags: SIM_RUSTFLAGS.to_owned(),
        },
        host: command_line(&mut Command::new("hostname")),
        jobs: flags.jobs,
        memory_max,
        summary: summarize(&results),
        cases: results,
    };
    fs::write(
        flags.out.join(REPORT_FILE),
        serde_json::to_string_pretty(&report)? + "\n",
    )?;
    for (property, summary) in &report.summary {
        println!(
            "{property}: {:?} {:?}",
            summary.results, summary.run_failures
        );
    }
    println!(
        "{} cases; report in {}",
        report.cases.len(),
        flags.out.join(REPORT_FILE).display()
    );
    Ok(())
}

/// The loop cases of `<dir>/cases.json`: its entries that name a property,
/// with patches relative to `dir`.
pub(super) fn native_cases(repo_root: &Path, dir: &Path) -> Result<Vec<LoopCase>, BoxError> {
    let listed_in = dir.join("cases.json");
    let entries: Vec<Value> = serde_json::from_slice(
        &fs::read(&listed_in).map_err(|error| format!("{}: {error}", listed_in.display()))?,
    )?;
    let mut cases = Vec::new();
    for value in entries
        .into_iter()
        .filter(|value| value.get("property").is_some())
    {
        let entry: CaseEntry = serde_json::from_value(value)
            .map_err(|error| format!("{}: {error}", listed_in.display()))?;
        let patch = dir.join(&entry.patch);
        cases.push(LoopCase {
            label: relative_label(repo_root, &patch),
            patch,
            listed_in: listed_in.clone(),
            property: entry.property,
            witness: entry.witness,
            entry: entry.entry,
            table: entry.table,
            also_violates: entry.also_violates,
        });
    }
    Ok(cases)
}

/// Every retained planted case whose property starts with one of
/// `prefixes`, with its own committed witness.
fn planted_cases(repo_root: &Path, prefixes: &[String]) -> Result<Vec<LoopCase>, BoxError> {
    if prefixes.is_empty() {
        return Ok(Vec::new());
    }
    let root = repo_root.join(PLANTED_DIR);
    let mut cases = Vec::new();
    let mut matched = BTreeSet::new();
    for patch in planted_layout::files(&root, "patch")? {
        let Some(property) = planted_layout::property_id(&root, &patch) else {
            continue;
        };
        let path = relative_label(&root, &patch);
        if !prefixes.iter().any(|prefix| path.starts_with(prefix)) {
            continue;
        }
        matched.extend(
            prefixes
                .iter()
                .filter(|prefix| path.starts_with(prefix.as_str())),
        );
        let case_file = patch.with_extension("toml");
        let CaseFile { also_violates, .. } = toml::from_str(&fs::read_to_string(&case_file)?)
            .map_err(|error| format!("{}: {error}", case_file.display()))?;
        cases.push(LoopCase {
            label: relative_label(repo_root, &patch),
            listed_in: case_file.clone(),
            witness: WitnessSpec::Planted {
                planted: PathBuf::from(relative_label(repo_root, &case_file)),
            },
            patch,
            property,
            entry: None,
            table: None,
            also_violates,
        });
    }
    if let Some(unmatched) = prefixes.iter().find(|prefix| !matched.contains(prefix)) {
        return Err(format!("--planted {unmatched} matches no case under {PLANTED_DIR}").into());
    }
    Ok(cases)
}

/// `path` relative to `repo_root`, or whole if it lies outside.
fn relative_label(repo_root: &Path, path: &Path) -> String {
    path.strip_prefix(repo_root)
        .unwrap_or(path)
        .to_string_lossy()
        .into_owned()
}

/// The cases `only` names by property, or all of them, then those a change
/// since `changed_since` affects. `None` if the change affects none. Fails
/// on zero cases before the change filter.
fn select(
    repo_root: &Path,
    cases: Vec<LoopCase>,
    only: &[String],
    changed_since: Option<&str>,
) -> Result<Option<Vec<LoopCase>>, BoxError> {
    if let Some(unknown) = only
        .iter()
        .find(|id| !cases.iter().any(|case| &case.property == *id))
    {
        return Err(format!("--only {unknown} names no loop case's property").into());
    }
    let cases: Vec<LoopCase> = cases
        .into_iter()
        .filter(|case| only.is_empty() || only.contains(&case.property))
        .collect();
    if cases.is_empty() {
        return Err("zero loop cases selected".into());
    }
    let Some(rev) = changed_since else {
        return Ok(Some(cases));
    };
    let changed = changed_since_rev(repo_root, rev)?;
    let mut affected = Vec::new();
    for case in cases {
        if is_affected(&watched(repo_root, &case)?, &changed) {
            affected.push(case);
        }
    }
    Ok((!affected.is_empty()).then_some(affected))
}

/// Every path whose change can change `case`'s result, relative to
/// `repo_root`: the patch, the file that lists the case, the witness's
/// files, the files the patch changes, a planted witness's checks, and the
/// shared machinery.
fn watched(repo_root: &Path, case: &LoopCase) -> Result<Vec<PathBuf>, BoxError> {
    let mut watched: Vec<PathBuf> = [&case.patch, &case.listed_in]
        .into_iter()
        .filter_map(|path| path.strip_prefix(repo_root).ok().map(Path::to_path_buf))
        .collect();
    watched.extend(SHARED.iter().map(PathBuf::from));
    watched.extend(patched_files(repo_root, &case.patch)?);
    match &case.witness {
        WitnessSpec::Planted { planted } => {
            watched.push(planted.clone());
            let file: CaseFile = toml::from_str(&fs::read_to_string(repo_root.join(planted))?)?;
            if let Some(input) = file.input {
                watched.push(input.with_extension("toml"));
                watched.push(input);
            }
            if let Some(TypedFile { scenario, plan, .. }) = file.typed {
                watched.extend([scenario, plan]);
            }
            watched.extend(file.checks);
        }
        WitnessSpec::Input { input } => {
            watched.push(input.with_extension("toml"));
            watched.push(input.clone());
        }
        WitnessSpec::Test { test_target, .. } => {
            watched.push(PathBuf::from(format!(
                "crates/zakura-sim/tests/{test_target}.rs"
            )));
        }
    }
    Ok(watched)
}

/// The host's memory, from `/proc/meminfo`.
fn host_memory() -> Result<u64, BoxError> {
    let text = fs::read_to_string("/proc/meminfo")?;
    text.lines()
        .find_map(|line| {
            let kib = line.strip_prefix("MemTotal:")?.trim().strip_suffix("kB")?;
            kib.trim().parse::<u64>().ok()
        })
        .map(|kib| kib * 1024)
        .ok_or_else(|| "/proc/meminfo has no MemTotal".into())
}

/// `command`'s first output line, or `unknown`.
fn command_line(command: &mut Command) -> String {
    command
        .output()
        .ok()
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .and_then(|text| text.lines().next().map(str::to_owned))
        .unwrap_or_else(|| "unknown".to_owned())
}

/// What every slot shares.
struct Runner {
    out: PathBuf,
    work: PathBuf,
    pinned: String,
    memory_max: Option<u64>,
    build_jobs: usize,
}

/// One worker's worktree and target directory.
struct Slot {
    tree: PathBuf,
    target: PathBuf,
}

/// The binaries one build made.
#[derive(Default)]
struct Built {
    sim: Option<PathBuf>,
    tests: BTreeMap<String, PathBuf>,
}

/// One witness run's evidence.
struct Ran {
    observed: Observed,
    millis: u64,
    peak_rss_kib: Option<u64>,
    log: PathBuf,
}

/// A mutant: one patch and its cases, by index into the case list.
struct Mutant {
    patch: PathBuf,
    cases: Vec<usize>,
}

impl Runner {
    fn slot(&self, index: usize) -> Slot {
        let dir = self.work.join(format!("slot-{index}"));
        Slot {
            tree: dir.join("tree"),
            target: dir.join("target"),
        }
    }

    /// Runs every case: the baselines on slot 0, then each mutant on the
    /// next free slot, then each case the memory cap killed alone. Returns
    /// the unpatched source digest and each case's report, in case order.
    fn run_all(
        &self,
        cases: &[LoopCase],
        slots: &[Slot],
    ) -> Result<(String, Vec<CaseReport>), BoxError> {
        let runnables = cases
            .iter()
            .map(|case| runnable(&slots[0].tree, &case.witness, &case.property))
            .collect::<Result<Vec<_>, _>>()?;
        let source = provenance::Provenance::read(&slots[0].tree, provenance::SOURCE_PATHS).source;
        let all: Vec<usize> = (0..cases.len()).collect();
        let host_jobs = thread::available_parallelism().map_or(1, usize::from);
        let baseline_dir = self.out.join("baseline");
        let built = match self.build(&slots[0], &runnables, &all, &baseline_dir, host_jobs)? {
            Ok(built) => built,
            Err(failure) => return Err(format!("the unpatched build fails: {failure}").into()),
        };
        // Every slot starts from slot 0's dependencies, so a slot's first
        // mutant rebuilds only the workspace crates.
        for slot in &slots[1..] {
            if slot.target.exists() {
                fs::remove_dir_all(&slot.target)?;
            }
            run_command(
                Command::new("cp")
                    .args(["-a", "--reflink=auto"])
                    .arg(&slots[0].target)
                    .arg(&slot.target),
            )?;
            // The copied build-script output watches slot 0's tree, whose
            // files reset to the pin, so a patch here wouldn't rerun the
            // script, and the binary would carry the unpatched source digest.
            // A newer `build.rs` reruns it once against this slot's tree.
            fs::File::options()
                .write(true)
                .open(slot.tree.join("crates/zakura-sim/build.rs"))?
                .set_modified(SystemTime::now())?;
        }
        let baselines = self.baselines(cases, &runnables, &slots[0], &built, &source)?;
        let mut mutants: BTreeMap<PathBuf, Vec<usize>> = BTreeMap::new();
        for (index, case) in cases.iter().enumerate() {
            mutants.entry(case.patch.clone()).or_default().push(index);
        }
        let queue: Mutex<VecDeque<Mutant>> = Mutex::new(
            mutants
                .into_iter()
                .map(|(patch, cases)| Mutant { patch, cases })
                .collect(),
        );
        let results: Mutex<BTreeMap<usize, CaseReport>> = Mutex::new(BTreeMap::new());
        thread::scope(|scope| {
            let workers: Vec<_> = slots
                .iter()
                .map(|slot| {
                    let (queue, results, baselines, runnables) =
                        (&queue, &results, &baselines, &runnables);
                    scope.spawn(move || -> Result<(), String> {
                        loop {
                            let Some(mutant) = queue.lock().expect("no worker panics").pop_front()
                            else {
                                return Ok(());
                            };
                            let reports = self
                                .mutant(slot, &mutant, cases, runnables, baselines, self.memory_max)
                                .map_err(|error| error.to_string())?;
                            results.lock().expect("no worker panics").extend(reports);
                        }
                    })
                })
                .collect();
            workers
                .into_iter()
                .map(|worker| {
                    worker
                        .join()
                        .expect("a worker returns its error, not a panic")
                })
                .collect::<Result<Vec<()>, String>>()
        })?;
        let mut results = results.into_inner().expect("every worker joined");
        // A case the memory cap killed reruns alone with the host's memory.
        let killed: Vec<usize> = results
            .iter()
            .filter(|(_, report)| report.result.reason == Some(Reason::Oom))
            .map(|(index, _)| *index)
            .collect();
        for index in killed {
            let mutant = Mutant {
                patch: cases[index].patch.clone(),
                cases: vec![index],
            };
            for (index, mut report) in self.mutant(
                &slots[0],
                &mutant,
                cases,
                &runnables,
                &baselines,
                self.alone_cap(),
            )? {
                report.rerun_alone = true;
                results.insert(index, report);
            }
        }
        Ok((source, results.into_values().collect()))
    }

    /// The memory cap of a run alone: the host's memory, or none if the
    /// loop runs without a cap.
    fn alone_cap(&self) -> Option<u64> {
        self.memory_max.and_then(|_| host_memory().ok())
    }

    /// Runs each distinct witness once on the unpatched `built`, and judges
    /// it for each case's property. A witness the memory cap killed reruns
    /// alone with [`Runner::alone_cap`].
    fn baselines(
        &self,
        cases: &[LoopCase],
        runnables: &[Runnable],
        slot: &Slot,
        built: &Built,
        source: &str,
    ) -> Result<Vec<BaselineReport>, BoxError> {
        let distinct: BTreeSet<usize> = (0..cases.len())
            .filter(|index| runnables[..*index].iter().all(|r| r != &runnables[*index]))
            .collect();
        let pending: Mutex<VecDeque<usize>> = Mutex::new(distinct.iter().copied().collect());
        let ran: Mutex<BTreeMap<usize, Ran>> = Mutex::new(BTreeMap::new());
        let workers = self.memory_max.map_or(1, |cap| {
            usize::try_from(host_memory().unwrap_or(cap) / cap.max(1)).unwrap_or(1)
        });
        thread::scope(|scope| {
            let handles: Vec<_> = (0..workers.max(1))
                .map(|_| {
                    scope.spawn(|| -> Result<(), String> {
                        loop {
                            let Some(index) = pending.lock().expect("no worker panics").pop_front()
                            else {
                                return Ok(());
                            };
                            let dir = self.out.join("baseline").join(format!("witness-{index}"));
                            let result = self
                                .witness(
                                    slot,
                                    built,
                                    &runnables[index],
                                    &dir,
                                    source,
                                    BASELINE_DEADLINE,
                                    self.memory_max,
                                )
                                .map_err(|error| error.to_string())?;
                            ran.lock().expect("no worker panics").insert(index, result);
                        }
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|handle| {
                    handle
                        .join()
                        .expect("a worker returns its error, not a panic")
                })
                .collect::<Result<Vec<()>, String>>()
        })?;
        let mut ran = ran.into_inner().expect("every worker joined");
        let killed: BTreeSet<usize> = ran
            .iter()
            .filter(|(_, ran)| classify::cap_killed(&ran.observed))
            .map(|(index, _)| *index)
            .collect();
        for index in &killed {
            let dir = self
                .out
                .join("baseline")
                .join(format!("witness-{index}-alone"));
            let rerun = self.witness(
                slot,
                built,
                &runnables[*index],
                &dir,
                source,
                BASELINE_DEADLINE,
                self.alone_cap(),
            )?;
            ran.insert(*index, rerun);
        }
        Ok((0..cases.len())
            .map(|index| {
                let first = *distinct
                    .iter()
                    .find(|first| runnables[**first] == runnables[index])
                    .expect("every runnable has a first case");
                let ran = &ran[&first];
                let detail = classify::baseline(&cases[index].property, &ran.observed);
                println!(
                    "baseline {} for {}: {}",
                    describe(&cases[index].witness),
                    cases[index].property,
                    detail.as_deref().unwrap_or("holds")
                );
                BaselineReport {
                    passed: detail.is_none(),
                    detail,
                    millis: ran.millis,
                    rerun_alone: killed.contains(&first),
                }
            })
            .collect())
    }

    /// Applies `mutant`'s patch in `slot`, builds what its cases need, and
    /// runs each case's witness under `cap`.
    #[allow(clippy::too_many_arguments)]
    fn mutant(
        &self,
        slot: &Slot,
        mutant: &Mutant,
        cases: &[LoopCase],
        runnables: &[Runnable],
        baselines: &[BaselineReport],
        cap: Option<u64>,
    ) -> Result<Vec<(usize, CaseReport)>, BoxError> {
        if INTERRUPTED.load(Ordering::SeqCst) {
            return Err(INTERRUPTED_ERROR.into());
        }
        let first = &cases[mutant.cases[0]];
        let dir = self.out.join("mutants").join(key(&first.label));
        fs::create_dir_all(&dir)?;
        let patch_sha256 = file_sha256(&mutant.patch).unwrap_or_default();
        let report = |index: usize, result: Classified, ran: Option<&Ran>| {
            let case = &cases[index];
            CaseReport {
                patch: case.label.clone(),
                patch_sha256: patch_sha256.clone(),
                property: case.property.clone(),
                entry: case.entry.clone(),
                table: case.table.clone(),
                witness: case.witness.clone(),
                also_violates: case.also_violates.clone(),
                result,
                trigger: ran.map(|ran| classify::trigger(&case.property, &ran.observed)),
                baseline: baselines[index].clone(),
                millis: ran.map(|ran| ran.millis),
                peak_rss_kib: ran.and_then(|ran| ran.peak_rss_kib),
                log: ran
                    .and_then(|ran| ran.log.strip_prefix(&self.out).ok().map(Path::to_path_buf)),
                rerun_alone: false,
            }
        };
        let all = |result: Classified| -> Vec<(usize, CaseReport)> {
            mutant
                .cases
                .iter()
                .map(|index| (*index, report(*index, result.clone(), None)))
                .collect()
        };
        reset(&slot.tree, &self.pinned)?;
        if let Err(why) = apply(&slot.tree, &mutant.patch) {
            return Ok(all(Classified::new(Class::PatchDoesNotApply, None, why)));
        }
        let live: Vec<usize> = mutant
            .cases
            .iter()
            .copied()
            .filter(|index| baselines[*index].passed)
            .collect();
        let mut reports: Vec<(usize, CaseReport)> = mutant
            .cases
            .iter()
            .filter(|index| !baselines[**index].passed)
            .map(|index| {
                let detail = baselines[*index].detail.clone().unwrap_or_default();
                (
                    *index,
                    report(
                        *index,
                        Classified::new(Class::BaselineFails, None, detail),
                        None,
                    ),
                )
            })
            .collect();
        if live.is_empty() {
            return Ok(reports);
        }
        let built = match self.build(slot, runnables, &live, &dir, self.build_jobs)? {
            Ok(built) => built,
            Err(BuildFailure::Rejected(why)) => {
                reports.extend(live.iter().map(|index| {
                    (
                        *index,
                        report(
                            *index,
                            Classified::new(Class::DidNotCompile, None, why.clone()),
                            None,
                        ),
                    )
                }));
                return Ok(reports);
            }
            Err(BuildFailure::Broken(why)) => {
                reports.extend(live.iter().map(|index| {
                    let result =
                        Classified::new(Class::HarnessFailure, Some(Reason::Build), why.clone());
                    (*index, report(*index, result, None))
                }));
                return Ok(reports);
            }
        };
        let source = provenance::Provenance::read(&slot.tree, provenance::SOURCE_PATHS).source;
        for index in live {
            let case = &cases[index];
            let deadline = (Duration::from_millis(baselines[index].millis) * DEADLINE_FACTOR)
                .max(MIN_DEADLINE);
            let ran = self.witness(
                slot,
                &built,
                &runnables[index],
                &dir.join(key(&case.property)),
                &source,
                deadline,
                cap,
            )?;
            let result = classify::mutant(&case.property, &case.also_violates, &ran.observed);
            println!(
                "{} {}: {}: {}",
                case.label,
                case.property,
                result.key(),
                result.detail
            );
            reports.push((index, report(index, result, Some(&ran))));
        }
        Ok(reports)
    }

    /// Builds in `slot`, with `jobs` compiler jobs, the `zakura-sim` binary
    /// if a case in `cases` runs a recorded witness, and each test binary
    /// they name. The binaries, or why the build failed.
    fn build(
        &self,
        slot: &Slot,
        runnables: &[Runnable],
        cases: &[usize],
        dir: &Path,
        jobs: usize,
    ) -> Result<Result<Built, BuildFailure>, BoxError> {
        fs::create_dir_all(dir)?;
        let needs_sim = cases
            .iter()
            .any(|index| matches!(runnables[*index], Runnable::Record { .. }));
        let targets: BTreeSet<&str> = cases
            .iter()
            .filter_map(|index| match &runnables[*index] {
                Runnable::Test { target, .. } => Some(target.as_str()),
                Runnable::Record { .. } => None,
            })
            .collect();
        let mut built = Built::default();
        let cargo = |args: &[&str]| {
            let mut command = Command::new(env::var_os("CARGO").unwrap_or_else(|| "cargo".into()));
            command
                .current_dir(&slot.tree)
                .env("RUSTFLAGS", SIM_RUSTFLAGS)
                .env("CARGO_TARGET_DIR", &slot.target)
                .env("CARGO_BUILD_WARNINGS", "warn")
                .env("CARGO_BUILD_JOBS", jobs.to_string())
                .args(args)
                .args([
                    "--locked",
                    "--profile",
                    PROFILE,
                    "--message-format=json",
                    "-p",
                    "zakura-sim",
                ]);
            command
        };
        if needs_sim {
            let log = dir.join("build-sim.log");
            let end = run_measured(&mut cargo(&["build", "--bin", "zakura-sim"]), &log, None)?.0;
            if end != Ended::Exited(0) {
                return Ok(Err(build_failure(end, &log)));
            }
            built.sim = Some(slot.target.join(PROFILE).join("zakura-sim"));
        }
        if !targets.is_empty() {
            let mut args = vec!["test", "--no-run"];
            for target in &targets {
                args.extend(["--test", target]);
            }
            let log = dir.join("build-tests.log");
            let end = run_measured(&mut cargo(&args), &log, None)?.0;
            if end != Ended::Exited(0) {
                return Ok(Err(build_failure(end, &log)));
            }
            built.tests = test_executables(&fs::read_to_string(&log)?);
            if let Some(missing) = targets
                .iter()
                .find(|target| !built.tests.contains_key(**target))
            {
                return Ok(Err(BuildFailure::Broken(format!(
                    "cargo built no test binary for {missing}; see {}",
                    log.display()
                ))));
            }
        }
        Ok(Ok(built))
    }

    /// Runs `runnable` from `slot`'s binaries `built`, with its records in
    /// `dir`, under `deadline` and the memory `cap`. Only records whose
    /// source digest is `source` count.
    #[allow(clippy::too_many_arguments)]
    fn witness(
        &self,
        slot: &Slot,
        built: &Built,
        runnable: &Runnable,
        dir: &Path,
        source: &str,
        deadline: Duration,
        cap: Option<u64>,
    ) -> Result<Ran, BoxError> {
        if dir.exists() {
            fs::remove_dir_all(dir)?;
        }
        let records = dir.join("records");
        fs::create_dir_all(&records)?;
        let log = dir.join("witness.log");
        let crate_dir = slot.tree.join("crates").join("zakura-sim");
        let (mut command, listed) = match runnable {
            Runnable::Record {
                harness,
                args,
                property,
            } => {
                let sim = built
                    .sim
                    .as_ref()
                    .ok_or("the build made no zakura-sim binary")?;
                let mut command = scoped(cap, sim);
                command
                    .args(["record", "--sim", harness])
                    .args(args)
                    .arg("--out")
                    .arg(dir.join("trace"))
                    .args(["--witness", property, "--expect", "holds"]);
                (command, None)
            }
            Runnable::Test { target, test } => {
                let binary = built
                    .tests
                    .get(target)
                    .ok_or_else(|| format!("the build made no test binary for {target}"))?;
                let listed = Command::new(binary)
                    .current_dir(&crate_dir)
                    .args(["--list", "--exact", test])
                    .output()?;
                let count = String::from_utf8_lossy(&listed.stdout)
                    .lines()
                    .filter(|line| line.ends_with(": test"))
                    .count();
                let mut command = scoped(cap, binary);
                command.args(["--exact", test, "--include-ignored", "--test-threads", "1"]);
                (command, Some(count))
            }
        };
        command
            .current_dir(&crate_dir)
            .env(RECORDS_VAR, &records)
            .env("CARGO_MANIFEST_DIR", &crate_dir);
        let started = Instant::now();
        let (ended, peak_rss_kib) = if listed == Some(0) {
            (Ended::Exited(0), None)
        } else {
            run_measured(&mut command, &log, Some(deadline))?
        };
        let millis = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        let (records, unpaired) = read_records(&records, source)?;
        Ok(Ran {
            observed: Observed {
                ended,
                listed,
                records,
                unpaired,
            },
            millis,
            peak_rss_kib,
            log,
        })
    }
}

/// How the runner runs `witness` for `property`, reading a planted case or
/// an input's sidecar from `tree`.
fn runnable(tree: &Path, witness: &WitnessSpec, property: &str) -> Result<Runnable, BoxError> {
    let input_args = |input: &Path| -> Result<Runnable, BoxError> {
        Ok(Runnable::Record {
            harness: super::harness_of(tree, input)?,
            args: vec![
                "--input".to_owned(),
                tree.join(input).to_string_lossy().into_owned(),
            ],
            property: property.to_owned(),
        })
    };
    match witness {
        WitnessSpec::Planted { planted } => {
            let path = tree.join(planted);
            let file: CaseFile = toml::from_str(
                &fs::read_to_string(&path)
                    .map_err(|error| format!("{}: {error}", path.display()))?,
            )
            .map_err(|error| format!("{}: {error}", path.display()))?;
            match (file.input, file.typed) {
                (Some(input), None) => input_args(&input),
                (
                    None,
                    Some(TypedFile {
                        harness,
                        scenario,
                        plan,
                    }),
                ) => Ok(Runnable::Record {
                    harness,
                    args: vec![
                        "--scenario".to_owned(),
                        fs::read_to_string(tree.join(&scenario))?,
                        "--plan".to_owned(),
                        tree.join(plan).to_string_lossy().into_owned(),
                    ],
                    property: property.to_owned(),
                }),
                _ => Err(format!(
                    "{}: names neither or both of input and typed",
                    path.display()
                )
                .into()),
            }
        }
        WitnessSpec::Input { input } => input_args(input),
        WitnessSpec::Test { test, test_target } => Ok(Runnable::Test {
            target: test_target.clone(),
            test: test.clone(),
        }),
    }
}

/// A case's witness in words.
fn describe(witness: &WitnessSpec) -> String {
    match witness {
        WitnessSpec::Planted { planted } => planted.display().to_string(),
        WitnessSpec::Input { input } => input.display().to_string(),
        WitnessSpec::Test { test, test_target } => format!("{test_target}::{test}"),
    }
}

/// A directory name for `text`: its characters outside `[A-Za-z0-9.-]`
/// become `_`, with a short hash so two names never collide.
fn key(text: &str) -> String {
    let readable: String = text
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '.' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect();
    let digest = hex(&Sha256::digest(text.as_bytes()));
    format!("{}-{}", readable.trim_start_matches('_'), &digest[..8])
}

/// Resets `tree` to `pinned`, removing every untracked file, so a patch
/// applies to the pinned source alone.
fn reset(tree: &Path, pinned: &str) -> Result<(), BoxError> {
    run_command(git(tree).args(["reset", "--hard", "--quiet", pinned]))?;
    run_command(git(tree).args(["clean", "-fdq"]))
}

/// The test executables a `cargo test --no-run --message-format=json` log
/// names, by test target.
pub(super) fn test_executables(log: &str) -> BTreeMap<String, PathBuf> {
    log.lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|message| {
            message.get("reason").and_then(Value::as_str) == Some("compiler-artifact")
                && message.pointer("/profile/test").and_then(Value::as_bool) == Some(true)
                && message
                    .pointer("/target/kind")
                    .and_then(Value::as_array)
                    .is_some_and(|kinds| kinds.iter().any(|kind| kind == "test"))
        })
        .filter_map(|message| {
            Some((
                message.pointer("/target/name")?.as_str()?.to_owned(),
                PathBuf::from(message.get("executable")?.as_str()?),
            ))
        })
        .collect()
}

/// Why a build that ended `end` and wrote `log` failed: rejected if rustc
/// reported an error at a source location, else broken.
fn build_failure(end: Ended, log: &Path) -> BuildFailure {
    let end = match end {
        Ended::Exited(code) => ChildEnd::Exited(code),
        Ended::Signaled(signal) => ChildEnd::Signaled(signal),
        Ended::TimedOut => ChildEnd::TimedOut,
    };
    let text = fs::read(log).unwrap_or_default();
    let why = format!("`cargo` {end}; see {}", log.display());
    sim_build_failure(end, &String::from_utf8_lossy(&text), why)
}

/// The records in `dir` that the build with source digest `source` wrote,
/// in the order written, and how many another build wrote.
fn read_records(dir: &Path, source: &str) -> Result<(Vec<classify::Record>, usize), BoxError> {
    let mut paths: Vec<PathBuf> = fs::read_dir(dir)?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<Result<_, io::Error>>()?;
    paths.retain(|path| {
        path.extension()
            .is_some_and(|extension| extension == "json")
    });
    paths.sort();
    let mut records = Vec::new();
    let mut unpaired = 0;
    for path in paths {
        match classify::read_record(&fs::read_to_string(&path)?, source)
            .map_err(|error| format!("{}: {error}", path.display()))?
        {
            Some(record) => records.push(record),
            None => unpaired += 1,
        }
    }
    Ok((records, unpaired))
}

/// `program` under `systemd-run --scope -p MemoryMax=<cap>`, or alone
/// without a cap. A user other than root runs it in their own manager.
fn scoped(cap: Option<u64>, program: &Path) -> Command {
    let Some(cap) = cap else {
        return Command::new(program);
    };
    let mut command = Command::new("systemd-run");
    if !is_root() {
        command.arg("--user");
    }
    command
        .args(["--scope", "--quiet", "--collect", "-p"])
        .arg(format!("MemoryMax={cap}"))
        .arg("--")
        .arg(program);
    command
}

#[cfg(unix)]
fn is_root() -> bool {
    // SAFETY: `geteuid` has no preconditions.
    unsafe { libc::geteuid() == 0 }
}

#[cfg(not(unix))]
fn is_root() -> bool {
    false
}

/// Runs `command` in a process group of its own, with stdin closed and its
/// output in `log`, until it exits or `deadline` passes. Returns how it
/// ended and its peak resident set in KiB. An interrupt kills its group and
/// stops the loop.
#[cfg(unix)]
fn run_measured(
    command: &mut Command,
    log: &Path,
    deadline: Option<Duration>,
) -> Result<(Ended, Option<u64>), BoxError> {
    use std::os::unix::process::CommandExt as _;

    if let Some(parent) = log.parent() {
        fs::create_dir_all(parent)?;
    }
    let file = fs::File::create(log)?;
    command
        .stdin(Stdio::null())
        .stdout(file.try_clone()?)
        .stderr(file)
        .process_group(0);
    let child = command
        .spawn()
        .map_err(|error| format!("starting {:?}: {error}", command.get_program()))?;
    let pid = libc::pid_t::try_from(child.id())?;
    let started = Instant::now();
    let kill = || {
        // SAFETY: `kill` has no memory-safety preconditions, and the group
        // is the child's own.
        unsafe {
            libc::kill(-pid, libc::SIGKILL);
        }
    };
    loop {
        let mut status = 0;
        // SAFETY: an all-zero `rusage` is a valid value of a plain C struct.
        let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
        let options = if INTERRUPTED.load(Ordering::SeqCst)
            || deadline.is_some_and(|deadline| started.elapsed() >= deadline)
        {
            kill();
            0
        } else {
            libc::WNOHANG
        };
        // SAFETY: `status` and `usage` are valid for writes, and `pid` is
        // our unreaped child.
        let reaped = unsafe { libc::wait4(pid, &mut status, options, &mut usage) };
        if reaped < 0 {
            return Err(io::Error::last_os_error().into());
        }
        if reaped == pid {
            // Descendants outlive the child only in its group.
            kill();
            // `ru_maxrss` is in KiB on Linux, and never negative.
            let peak = u64::try_from(usage.ru_maxrss).ok();
            if options == 0 {
                if INTERRUPTED.load(Ordering::SeqCst) {
                    return Err(INTERRUPTED_ERROR.into());
                }
                return Ok((Ended::TimedOut, peak));
            }
            let ended = if libc::WIFEXITED(status) {
                Ended::Exited(libc::WEXITSTATUS(status))
            } else {
                Ended::Signaled(libc::WTERMSIG(status))
            };
            return Ok((ended, peak));
        }
        thread::sleep(POLL);
    }
}

#[cfg(not(unix))]
fn run_measured(
    _: &mut Command,
    _: &Path,
    _: Option<Duration>,
) -> Result<(Ended, Option<u64>), BoxError> {
    Err("the mutation loop runs only on Unix".into())
}
