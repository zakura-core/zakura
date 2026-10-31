//! Target readiness: each simulator fuzz target built, run on its harness's
//! curated seeds, and held to the harness the fuzz package maps it to
//! (explorer implementation plan, E5a).
//!
//! The targets are `qa/suite.toml`'s `[[target]]` rows in the simulator
//! fuzz package, the C1 suite manifest's target rows. For each one,
//! [`check_targets`]:
//!
//! 1. resolves the target to its harness through the fuzz package's
//!    `[package.metadata.zakura-sim.targets]` table and [`HARNESSES`];
//! 2. loads the harness's committed seeds ([`seed::load`]), and runs each
//!    admitted seed in this process on a [`Worker`], counting which of the
//!    harness's properties applied;
//! 3. builds the target with [`build_fuzzer`];
//! 4. runs the built target once on the admitted seeds, as libFuzzer runs a
//!    list of files, with the suite row's libFuzzer arguments and variables
//!    and [`STATS_VAR`] set;
//! 5. reads the process's stats file, which must name the mapped harness,
//!    be complete, and count at least one run per seed.
//!
//! Each target gets one [`TargetStatus`], and each status means one thing.
//! A target is ready only if every step passed and at least one of its
//! harness's own properties, other than the ones every harness checks
//! ([`SHARED_PROPERTIES`]), applied in a seed's run. A run of seeds shows
//! that the target builds and runs its harness. It says nothing about how
//! well the target searches.
//!
//! Each row also lists the retained production bypasses under
//! `qa/planted/` whose committed input runs on the target's harness. `cargo
//! xtask planted --targets` runs each one through its target.

use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Command,
    time::Duration,
};

use serde::{Deserialize, Serialize};

use super::{
    build::{build_fuzzer, target_harness, FUZZ_PACKAGE},
    run::run_logged,
    store::CampaignError,
};
use crate::{
    explore::{
        stats::{Counts, ProcessStats, STATS_VAR},
        ProcessExit, ProcessFailure, Worker, ABANDON_LIMIT_VAR, EXPECT_VAR,
    },
    harness::{HarnessEntry, HARNESSES, NO_TASK_PANIC},
    seed::{self, sha256, Seed},
    sim::NO_LIVELOCK,
    suite::{SuiteFile, PLANTED_DIR, SIM_FUZZ_PACKAGE, SUITE_FILE},
    trace::ARTIFACTS_VAR,
};

/// The report's file in the output directory.
pub const READINESS_FILE: &str = "readiness.json";

/// The report's format version.
pub const READINESS_VERSION: u32 = 1;

/// The properties every harness checks. A target whose seeds apply only
/// these runs no property of its own harness.
pub const SHARED_PROPERTIES: [&str; 2] = [NO_TASK_PANIC, NO_LIVELOCK];

/// What libFuzzer prints once it has run one input file, before the path.
const EXECUTED: &str = "Executed ";

/// How long past its inputs' timeouts the target may run before it is
/// killed, so libFuzzer's own timeout report comes first.
const GRACE: Duration = Duration::from_secs(30);

/// How [`check_targets`] runs.
#[derive(Clone, Debug)]
pub struct TargetsOptions {
    /// The checkout whose targets it checks.
    pub repo: PathBuf,
    /// Where it writes each target's files and the report. Missing or
    /// empty.
    pub out: PathBuf,
    /// The targets to check, by name. Empty checks every target.
    pub targets: Vec<String>,
    /// cargo-fuzz's `--sanitizer`.
    pub sanitizer: String,
    /// cargo-fuzz's target directory.
    pub target_dir: PathBuf,
    /// The seed directories.
    pub fixtures: Vec<PathBuf>,
    /// libFuzzer's `-timeout` for one input.
    pub child: Duration,
}

/// One target's result. Each status means one thing.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TargetStatus {
    /// The target built, accepted every seed, ran the harness its manifest
    /// maps it to, counted every seed, and at least one of the harness's
    /// own properties applied in a seed's run.
    Ready,
    /// As [`TargetStatus::Ready`], except that no seed's run applied any of
    /// the harness's own properties: the seeds reach only
    /// [`SHARED_PROPERTIES`].
    NoApplicableProperty,
    /// The fuzz package maps the target to no harness, or to a harness
    /// that isn't registered.
    Unresolved,
    /// A required seed of the harness failed validation.
    SeedsRejected,
    /// The harness has no admitted seed.
    NoSeeds,
    /// `cargo fuzz build` failed, or its binary couldn't be read.
    BuildFailed,
    /// The target process failed on the seeds: it panicked on a failure,
    /// crashed, ran out of memory or time, or exited with an error.
    SeedsFailed,
    /// The target's stats file names another harness than its manifest.
    WrongHarness,
    /// The target exited cleanly, but its output and stats don't account
    /// for every seed: no stats file or more than one, an incomplete file,
    /// fewer runs than seeds, or a seed libFuzzer didn't report running.
    Unaccounted,
}

impl TargetStatus {
    /// The status's JSON name.
    pub fn name(self) -> &'static str {
        match self {
            TargetStatus::Ready => "ready",
            TargetStatus::NoApplicableProperty => "no-applicable-property",
            TargetStatus::Unresolved => "unresolved",
            TargetStatus::SeedsRejected => "seeds-rejected",
            TargetStatus::NoSeeds => "no-seeds",
            TargetStatus::BuildFailed => "build-failed",
            TargetStatus::SeedsFailed => "seeds-failed",
            TargetStatus::WrongHarness => "wrong-harness",
            TargetStatus::Unaccounted => "unaccounted",
        }
    }
}

/// One seed's run in this process.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SeedRun {
    /// The seed's file, relative to the repository's root.
    pub file: PathBuf,
    /// The SHA-256 of its full fuzz input.
    pub sha256: String,
    /// What the explorer did with its run, such as `clean`.
    pub outcome: String,
    /// The properties whose evaluation applied in its run.
    pub applied: Vec<String>,
}

/// A retained production bypass whose committed input runs on the target's
/// harness.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BypassCase {
    /// The property the patch violates.
    pub property: String,
    /// The patch, relative to the repository's root.
    pub patch: PathBuf,
}

/// One target's row in the report.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TargetReadiness {
    /// The target's `[[bin]]` name.
    pub target: String,
    /// The harness its manifest maps it to, if one.
    pub harness: Option<String>,
    /// Its status.
    pub status: TargetStatus,
    /// What the status is about, for every status but
    /// [`TargetStatus::Ready`].
    pub detail: Option<String>,
    /// Each admitted seed's run in this process.
    pub seeds: Vec<SeedRun>,
    /// The harness's own properties that applied in some seed's run.
    pub applied: Vec<String>,
    /// The harness's own properties that applied in no seed's run.
    pub unapplied: Vec<String>,
    /// The runs the target's stats file counted, if it was read.
    pub fuzzer_runs: Option<u64>,
    /// The retained production bypasses on the harness. Empty if none.
    pub bypasses: Vec<BypassCase>,
}

/// The report, [`READINESS_FILE`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Readiness {
    /// [`READINESS_VERSION`].
    pub version: u32,
    /// cargo-fuzz's sanitizer.
    pub sanitizer: String,
    /// One row per target, in `qa/suite.toml`'s order.
    pub targets: Vec<TargetReadiness>,
}

impl Readiness {
    /// The rows that aren't [`TargetStatus::Ready`].
    pub fn not_ready(&self) -> impl Iterator<Item = &TargetReadiness> {
        self.targets
            .iter()
            .filter(|row| row.status != TargetStatus::Ready)
    }
}

/// A suite row's target, with what its smoke run passes libFuzzer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeclaredTarget {
    /// The target's `[[bin]]` name.
    pub name: String,
    /// More libFuzzer arguments, such as `-max_len=4096`.
    pub args: Vec<String>,
    /// Variables the run needs, as `NAME=value`.
    pub env: Vec<String>,
}

/// The simulator fuzz package's targets that `suite`, `qa/suite.toml`,
/// runs, in its order.
pub fn declared_targets(suite: &SuiteFile) -> Vec<DeclaredTarget> {
    suite
        .target
        .iter()
        .filter(|target| target.package == SIM_FUZZ_PACKAGE)
        .map(|target| DeclaredTarget {
            name: target.name.clone(),
            args: target.args.clone(),
            env: target.env.clone(),
        })
        .collect()
}

/// The harness `target` runs, from the fuzz package's manifest text
/// `manifest`, or why it doesn't resolve to a registered harness.
pub fn resolve(manifest: &str, target: &str) -> Result<&'static HarnessEntry, String> {
    let Some(name) = target_harness(manifest, target) else {
        return Err(format!(
            "{FUZZ_PACKAGE}/Cargo.toml maps `{target}` to no harness in \
             [package.metadata.zakura-sim.targets]"
        ));
    };
    HARNESSES
        .iter()
        .find(|entry| entry.name == name)
        .ok_or_else(|| format!("`{target}` maps to `{name}`, which no harness registers"))
}

/// Runs `input` on `entry`'s harness on `worker`, and returns what the
/// explorer did with it and the properties whose evaluation applied.
pub fn run_seed(
    worker: &mut Worker,
    entry: &'static HarnessEntry,
    input: &[u8],
) -> (String, Vec<String>) {
    worker.explore(entry, input, move |_, run, classified| {
        let mut counts = Counts::for_harness(entry);
        counts.record(run, classified, &[]);
        let outcome = counts
            .outcomes
            .keys()
            .next()
            .cloned()
            .unwrap_or_else(|| "none".to_owned());
        let applied = counts
            .properties
            .into_iter()
            .filter(|(_, counts)| counts.applied())
            .map(|(id, _)| id)
            .collect();
        (outcome, applied)
    })
}

/// The harness's own properties, other than [`SHARED_PROPERTIES`], split
/// into those that applied in some seed's run and those that didn't.
pub fn own_applicability(entry: &HarnessEntry, seeds: &[SeedRun]) -> (Vec<String>, Vec<String>) {
    (entry.properties)()
        .into_iter()
        .map(|property| property.id)
        .filter(|id| !SHARED_PROPERTIES.contains(&id.as_str()))
        .partition(|id| seeds.iter().any(|seed| seed.applied.contains(id)))
}

/// What the target's run on `seeds` came to, judged from how it exited,
/// its output `log`, and the stats files it left, `stats`. Only the
/// `harness` the manifest maps the target to and the `applied` own
/// properties of the seeds' in-process runs are needed besides.
///
/// The run must exit cleanly, report each seed's file as run, and leave
/// exactly one complete stats file that names `harness` and counts at least
/// one run per seed. libFuzzer runs a list of files without the empty
/// input it runs before fuzzing. It runs an input a second time when the
/// input allocated more than it freed, to check for a leak, so a seed can
/// count twice.
pub fn judge_run(
    harness: &str,
    seeds: &[PathBuf],
    exit: ProcessExit,
    log: &[u8],
    stats: &[Result<ProcessStats, String>],
    applied: &[String],
) -> (TargetStatus, Option<String>) {
    if let Some(failure) = ProcessFailure::of(exit, log) {
        let text = String::from_utf8_lossy(log);
        let panic = text
            .lines()
            .rev()
            .find(|line| line.contains("panicked at") || line.starts_with("thread '"))
            .map_or_else(String::new, |line| format!("; {}", line.trim()));
        return (
            TargetStatus::SeedsFailed,
            Some(format!("the target failed: {failure:?}{panic}")),
        );
    }
    let stats = match stats {
        [] => {
            return (
                TargetStatus::Unaccounted,
                Some("the target left no stats file".to_owned()),
            )
        }
        [stats] => stats,
        more => {
            return (
                TargetStatus::Unaccounted,
                Some(format!(
                    "the target left {} stats files, not one",
                    more.len()
                )),
            )
        }
    };
    let stats = match stats {
        Ok(stats) => stats,
        Err(error) => return (TargetStatus::Unaccounted, Some(error.clone())),
    };
    if stats.harness != harness {
        return (
            TargetStatus::WrongHarness,
            Some(format!(
                "the target ran `{}`, and its manifest maps it to `{harness}`",
                stats.harness
            )),
        );
    }
    if !stats.complete {
        return (
            TargetStatus::Unaccounted,
            Some("the target's stats file isn't complete".to_owned()),
        );
    }
    if stats.counts.runs < seeds.len() as u64 {
        return (
            TargetStatus::Unaccounted,
            Some(format!(
                "the target counted {} runs, fewer than its {} seeds",
                stats.counts.runs,
                seeds.len()
            )),
        );
    }
    let text = String::from_utf8_lossy(log);
    if let Some(missing) = seeds.iter().find(|seed| {
        let line = format!("{EXECUTED}{}", seed.display());
        !text.lines().any(|reported| reported.starts_with(&line))
    }) {
        return (
            TargetStatus::Unaccounted,
            Some(format!(
                "libFuzzer didn't report running {}",
                missing.display()
            )),
        );
    }
    if applied.is_empty() {
        return (
            TargetStatus::NoApplicableProperty,
            Some("no seed's run applied a property of the harness's own".to_owned()),
        );
    }
    (TargetStatus::Ready, None)
}

/// The retained production bypasses under `repo`'s `qa/planted/` whose
/// committed input is one of `seeds`, by the input's path.
pub fn bypasses(repo: &Path, seeds: &[Seed]) -> Result<Vec<BypassCase>, String> {
    #[derive(Deserialize)]
    struct CaseFile {
        input: PathBuf,
    }
    let mut cases = Vec::new();
    let root = repo.join(PLANTED_DIR);
    for family in sorted(&root)? {
        for property in sorted(&family)? {
            for case in sorted(&property)? {
                if case.extension().is_none_or(|extension| extension != "toml") {
                    continue;
                }
                let text = fs::read_to_string(&case)
                    .map_err(|error| format!("reading {}: {error}", case.display()))?;
                let file: CaseFile = toml::from_str(&text)
                    .map_err(|error| format!("parsing {}: {error}", case.display()))?;
                let input = repo.join(&file.input);
                if !seeds.iter().any(|seed| same_file(&seed.path, &input)) {
                    continue;
                }
                let id = property
                    .strip_prefix(&root)
                    .map_err(|_| format!("{} is outside {PLANTED_DIR}", property.display()))?;
                cases.push(BypassCase {
                    property: id.to_string_lossy().into_owned(),
                    patch: Path::new(PLANTED_DIR)
                        .join(id)
                        .join(case.with_extension("patch").file_name().unwrap_or_default()),
                });
            }
        }
    }
    Ok(cases)
}

/// Checks each target `options` names, calls `checked` with each row as
/// it's done, writes the report into `options.out`, and returns it. Fails
/// only if the checker itself can't go on, such as on an output directory
/// that isn't empty.
pub fn check_targets(
    options: &TargetsOptions,
    mut checked: impl FnMut(&TargetReadiness),
) -> Result<Readiness, CampaignError> {
    let out = &options.out;
    fs::create_dir_all(out).map_err(CampaignError::io(format!("creating {}", out.display())))?;
    if fs::read_dir(out)
        .map_err(CampaignError::io(format!("listing {}", out.display())))?
        .next()
        .is_some()
    {
        return Err(CampaignError::Refused(format!(
            "{} isn't empty",
            out.display()
        )));
    }
    let suite_path = options.repo.join(SUITE_FILE);
    let suite: SuiteFile = toml::from_str(&read(&suite_path)?)
        .map_err(|error| CampaignError::corrupt(&suite_path, error.to_string()))?;
    let manifest = read(&options.repo.join(FUZZ_PACKAGE).join("Cargo.toml"))?;
    let declared = declared_targets(&suite);
    for name in &options.targets {
        if !declared.iter().any(|target| &target.name == name) {
            return Err(CampaignError::Refused(format!(
                "{SUITE_FILE} declares no simulator fuzz target `{name}`"
            )));
        }
    }
    let mut report = Readiness {
        version: READINESS_VERSION,
        sanitizer: options.sanitizer.clone(),
        targets: Vec::new(),
    };
    for target in declared
        .iter()
        .filter(|target| options.targets.is_empty() || options.targets.contains(&target.name))
    {
        let row = check_target(options, &manifest, target)?;
        checked(&row);
        report.targets.push(row);
    }
    let path = out.join(READINESS_FILE);
    let json = serde_json::to_string_pretty(&report)
        .map_err(|error| CampaignError::Refused(format!("serializing the report: {error}")))?;
    fs::write(&path, format!("{json}\n"))
        .map_err(CampaignError::io(format!("writing {}", path.display())))?;
    Ok(report)
}

/// One target's row, with its files under `<out>/<target>/`.
fn check_target(
    options: &TargetsOptions,
    manifest: &str,
    target: &DeclaredTarget,
) -> Result<TargetReadiness, CampaignError> {
    let mut row = TargetReadiness {
        target: target.name.clone(),
        harness: target_harness(manifest, &target.name),
        status: TargetStatus::Ready,
        detail: None,
        seeds: Vec::new(),
        applied: Vec::new(),
        unapplied: Vec::new(),
        fuzzer_runs: None,
        bypasses: Vec::new(),
    };
    let entry = match resolve(manifest, &target.name) {
        Ok(entry) => entry,
        Err(why) => return Ok(row.with(TargetStatus::Unresolved, why)),
    };
    let loaded = seed::load(&options.fixtures, Some(entry.name))
        .map_err(CampaignError::io("listing the seed directories"))?;
    row.bypasses = bypasses(&options.repo, &loaded.admitted).map_err(CampaignError::Refused)?;
    if let Some(failed) = loaded.rejected.first() {
        return Ok(row.with(
            TargetStatus::SeedsRejected,
            format!(
                "{} required seeds failed validation; {}: {}",
                loaded.rejected.len(),
                failed.path.display(),
                failed.error
            ),
        ));
    }
    if loaded.admitted.is_empty() {
        return Ok(row.with(
            TargetStatus::NoSeeds,
            format!("{} has no admitted seed", entry.name),
        ));
    }

    let dir = options.out.join(&target.name);
    let seeds_dir = dir.join("seeds");
    let stats_dir = dir.join("stats");
    for sub in [&seeds_dir, &stats_dir] {
        fs::create_dir_all(sub)
            .map_err(CampaignError::io(format!("creating {}", sub.display())))?;
    }
    let mut worker = Worker::new();
    let mut files = Vec::new();
    for seed in &loaded.admitted {
        let (outcome, applied) = run_seed(&mut worker, entry, &seed.input);
        let digest = sha256(&seed.input);
        let file = seeds_dir.join(&digest);
        fs::write(&file, &seed.input)
            .map_err(CampaignError::io(format!("writing {}", file.display())))?;
        files.push(file);
        row.seeds.push(SeedRun {
            file: seed::from_root(&seed.path),
            sha256: digest,
            outcome,
            applied,
        });
    }
    (row.applied, row.unapplied) = own_applicability(entry, &row.seeds);

    let fuzzer = dir.join("fuzzer");
    match build_fuzzer(
        &options.repo,
        &target.name,
        &options.sanitizer,
        &options.target_dir,
    ) {
        Ok((bytes, _)) => {
            fs::write(&fuzzer, bytes)
                .and_then(|()| fs::set_permissions(&fuzzer, fs::Permissions::from_mode(0o755)))
                .map_err(CampaignError::io(format!("writing {}", fuzzer.display())))?;
        }
        Err(error) => return Ok(row.with(TargetStatus::BuildFailed, error.to_string())),
    }

    let mut command = Command::new(&fuzzer);
    command
        .arg(format!("-timeout={}", options.child.as_secs().max(1)))
        .arg(format!(
            "-artifact_prefix={}/",
            dir.join("artifacts").display()
        ))
        .args(&target.args)
        .args(&files)
        .env(STATS_VAR, &stats_dir)
        .env(ARTIFACTS_VAR, dir.join("sim"))
        .env_remove(EXPECT_VAR)
        .env_remove(ABANDON_LIMIT_VAR);
    for variable in &target.env {
        let (name, value) = variable.split_once('=').unwrap_or((variable, ""));
        command.env(name, value);
    }
    fs::create_dir_all(dir.join("artifacts"))
        .map_err(CampaignError::io(format!("creating {}", dir.display())))?;
    // Each seed gets libFuzzer's timeout twice, for its leak check's rerun.
    let deadline = options.child * (2 * files.len() as u32) + GRACE;
    let (exit, log) = run_logged(command, &dir.join("run.log"), Some(deadline), None, |_| {})?;
    let stats: Vec<Result<ProcessStats, String>> = sorted(&stats_dir)
        .map_err(CampaignError::Refused)?
        .iter()
        .map(|path| ProcessStats::read(path))
        .collect();
    row.fuzzer_runs = stats
        .iter()
        .find_map(|stats| stats.as_ref().ok().map(|stats| stats.counts.runs));
    let (status, detail) = judge_run(entry.name, &files, exit, &log, &stats, &row.applied);
    row.status = status;
    row.detail = detail;
    Ok(row)
}

impl TargetReadiness {
    /// The row in words: its target, harness, status and detail, the
    /// harness's own properties that applied and those that didn't, and its
    /// retained bypasses, one line each.
    pub fn lines(&self) -> Vec<String> {
        let mut lines = vec![format!(
            "{} ({}): {}{}",
            self.target,
            self.harness.as_deref().unwrap_or("no harness"),
            self.status.name(),
            self.detail
                .as_ref()
                .map_or_else(String::new, |detail| format!(": {detail}"))
        )];
        if !self.applied.is_empty() {
            lines.push(format!("  applied: {}", self.applied.join(", ")));
        }
        if !self.unapplied.is_empty() {
            lines.push(format!(
                "  applied in no seed: {}",
                self.unapplied.join(", ")
            ));
        }
        if self.bypasses.is_empty() {
            lines.push("  no retained production bypass".to_owned());
        }
        for bypass in &self.bypasses {
            lines.push(format!(
                "  retained bypass: {} {}",
                bypass.property,
                bypass.patch.display()
            ));
        }
        lines
    }

    /// The row with `status` and `detail`.
    fn with(mut self, status: TargetStatus, detail: String) -> Self {
        self.status = status;
        self.detail = Some(detail);
        self
    }
}

/// The text of the file at `path`.
fn read(path: &Path) -> Result<String, CampaignError> {
    fs::read_to_string(path).map_err(CampaignError::io(format!("reading {}", path.display())))
}

/// The entries of `dir`, sorted. A missing directory has none.
fn sorted(dir: &Path) -> Result<Vec<PathBuf>, String> {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(format!("listing {}: {error}", dir.display())),
    };
    let mut paths = entries
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("listing {}: {error}", dir.display()))?;
    paths.sort();
    Ok(paths)
}

/// True if `a` and `b` name the same file.
fn same_file(a: &Path, b: &Path) -> bool {
    match (fs::canonicalize(a), fs::canonicalize(b)) {
        (Ok(a), Ok(b)) => a == b,
        _ => a == b,
    }
}
