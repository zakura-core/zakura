//! Run manifests: what a run fuzzes, with which build, budget and corpus,
//! and the `start` and `resume` commands that write them.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

use serde::{Deserialize, Serialize};

use super::{
    build::FuzzBuild,
    run::{close, fuzzer_command, read_end, run_logged, verify_end, Prepared},
    snapshot::Source,
    store::{
        now_unix, partial_path, read_record, record_bytes, write_once, CampaignError, Digest,
        RunState, Store, STORE_VERSION,
    },
};
use crate::{
    explore::{
        feedback::{FEEDBACK_LINE, FEEDBACK_VAR},
        ProcessFailure,
    },
    harness::HarnessEntry,
    seed,
    trace::{
        meta::{BudgetRecord, PropertyRef, SchedulerRecord, UNKNOWN},
        BuildInfo,
    },
};

/// The sanitizer variables a run's fuzzer processes get from its
/// manifest, and from nowhere else.
pub const SANITIZER_VARS: &[&str] = &[
    "ASAN_OPTIONS",
    "LSAN_OPTIONS",
    "MSAN_OPTIONS",
    "TSAN_OPTIONS",
    "UBSAN_OPTIONS",
];

/// How long the startup probe may take.
const PROBE_TIMEOUT: Duration = Duration::from_secs(120);

/// libFuzzer flags the supervisor sets itself, or that would change what a
/// worker is, so a manifest's extra arguments can't hold them.
const RESERVED_FLAGS: &[&str] = &[
    "-seed",
    "-max_total_time",
    "-artifact_prefix",
    "-exact_artifact_path",
    "-runs",
    "-merge",
    "-set_cover_merge",
    "-fork",
    "-jobs",
    "-workers",
    "-max_len",
    "-timeout",
    "-rss_limit_mb",
    "-minimize_crash",
    "-cleanse_crash",
];

/// One run: what it fuzzes, with which build, budget and corpus, and where
/// it comes from. Written once under its digest, and never changed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    /// [`STORE_VERSION`].
    pub version: u32,
    /// The campaign's name.
    pub campaign: String,
    /// The campaign segment, from 1. A new build, generator, format or
    /// contract version starts a new segment.
    pub segment: u32,
    /// The last run of the previous segment, for a segment after the first.
    pub previous_segment: Option<Digest>,
    /// How this segment's corpus came from the previous segment's, for a
    /// segment after the first.
    pub migration: Option<Migration>,
    /// The run this one resumes, in the same segment. `None` for a
    /// segment's first run.
    pub resume_parent: Option<Digest>,
    /// When the manifest was written, in seconds since the Unix epoch.
    pub created_unix: u64,
    /// The fuzzer.
    pub build: FuzzBuild,
    /// The `zakura-sim` binary that wrote the manifest, whose harness
    /// registry gave [`Manifest::harness`].
    pub runner: BuildInfo,
    /// The harness the fuzz target runs.
    pub harness: HarnessRecord,
    /// The semantic feedback the fuzzer logged at startup. `None` if it
    /// logged none, as a target without a collector does; libFuzzer then
    /// reads code edges alone.
    pub feedback: Option<FeedbackRecord>,
    /// The SHA-256 of the full-suite manifest the run belongs to, if any.
    pub suite_sha256: Option<Digest>,
    /// Each worker's budget and libFuzzer settings.
    pub budget: Budget,
    /// The workers.
    pub workers: Vec<WorkerPlan>,
    /// The snapshot every worker starts from.
    pub initial_snapshot: Digest,
    /// The curated seeds in the first run's initial snapshot.
    pub seeds: SeedsRecord,
    /// Where the operator copies the store, such as an object-store URI.
    /// The store doesn't read it.
    pub artifacts: Option<String>,
}

/// How a segment's corpus came from the previous segment's.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Migration {
    /// The previous segment's last snapshot, plus the curated seeds. Only
    /// when the harness's versions didn't change and both segments' fuzzer
    /// builds are clean, so every input keeps its meaning.
    Carry,
    /// The curated seeds alone. The previous segment's snapshots stay in
    /// the store.
    Seeds,
}

/// The harness a run fuzzes, from the runner's registry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HarnessRecord {
    /// The harness's name.
    pub name: String,
    /// [`seed::versions`]: what fixes the meaning of a fuzz input.
    pub versions: BTreeMap<String, String>,
    /// Every property the harness checks, with its status.
    pub properties: Vec<PropertyRef>,
    /// The simulator limits of the scenario the all-zero prefix draws. A
    /// harness whose limits depend on the scenario records each run's in
    /// its trace's `meta` record.
    pub limits: LimitsRecord,
}

impl HarnessRecord {
    /// `entry`'s record.
    pub fn of(entry: &HarnessEntry) -> Result<HarnessRecord, CampaignError> {
        let scenario = (entry.draw)(&vec![0; entry.scenario_bytes]);
        let config = (entry.config)(&scenario).map_err(|error| {
            CampaignError::Refused(format!(
                "{}'s all-zero scenario has no run config: {error}",
                entry.name
            ))
        })?;
        Ok(HarnessRecord {
            name: entry.name.to_owned(),
            versions: seed::versions(entry)
                .into_iter()
                .map(|(key, value)| (key.to_owned(), value))
                .collect(),
            properties: (entry.properties)(),
            limits: LimitsRecord {
                scheduler: config.scheduler.into(),
                steps: (&config).into(),
            },
        })
    }
}

/// A run's simulator limits.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LimitsRecord {
    /// The scheduler's limits.
    pub scheduler: SchedulerRecord,
    /// The step limits.
    pub steps: BudgetRecord,
}

/// The semantic feedback a fuzzer logged at startup.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedbackRecord {
    /// The feedback backend, such as `libfuzzer-extra-counters`.
    pub backend: String,
    /// The backend's version.
    pub version: String,
    /// `on` or `off`.
    pub mode: String,
    /// Every `key=value` pair of the line, the three above included.
    pub logged: BTreeMap<String, String>,
}

impl FeedbackRecord {
    /// The record of the first [`FEEDBACK_LINE`] in `log`. `None` if there
    /// is none. Fails if the line lacks the backend, version or mode.
    pub fn parse(log: &str) -> Result<Option<FeedbackRecord>, String> {
        let Some(line) = log
            .lines()
            .find_map(|line| line.split_once(FEEDBACK_LINE).map(|(_, rest)| rest))
        else {
            return Ok(None);
        };
        let logged: BTreeMap<String, String> = line
            .split_whitespace()
            .filter_map(|pair| pair.split_once('='))
            .map(|(key, value)| (key.to_owned(), value.to_owned()))
            .collect();
        let field = |key: &str| {
            logged
                .get(key)
                .cloned()
                .ok_or_else(|| format!("the feedback line `{line}` has no `{key}`"))
        };
        Ok(Some(FeedbackRecord {
            backend: field("backend")?,
            version: field("version")?,
            mode: field("mode")?,
            logged,
        }))
    }
}

/// Each worker's budget and libFuzzer settings.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Budget {
    /// Each worker's fuzzing time, in seconds, summed over its processes.
    pub seconds: u64,
    /// How many times a worker restarts a fuzzer process that ended before
    /// its budget did.
    pub max_restarts: u32,
    /// libFuzzer's `-max_len`.
    pub max_len: usize,
    /// libFuzzer's `-timeout`, per input, in seconds.
    pub timeout_secs: u64,
    /// libFuzzer's `-rss_limit_mb`.
    pub rss_limit_mb: u64,
    /// The target's abandonment limit, `ZAKURA_SIM_ABANDON_LIMIT`.
    pub abandon_limit: usize,
    /// `ZAKURA_SIM_FEEDBACK`, if set: `on` or `off`.
    pub feedback_mode: Option<String>,
    /// The sanitizer variables, from [`SANITIZER_VARS`], that the fuzzer
    /// processes run with. The others are unset.
    pub sanitizer_options: BTreeMap<String, String>,
    /// If set, the time limit in seconds for compacting the run's snapshot
    /// with libFuzzer's merge after the workers stop.
    pub compact_secs: Option<u64>,
    /// More libFuzzer flags, each `-name=value`. None of them is reserved.
    pub libfuzzer_args: Vec<String>,
}

impl Budget {
    /// Checks the budget: a nonzero time, a known feedback mode, known
    /// sanitizer variables, and extra flags that are flags and aren't
    /// reserved.
    fn check(&self) -> Result<(), CampaignError> {
        if let Some(name) = self
            .sanitizer_options
            .keys()
            .find(|name| !SANITIZER_VARS.contains(&name.as_str()))
        {
            return Err(CampaignError::Refused(format!(
                "`{name}` isn't a sanitizer variable: {}",
                SANITIZER_VARS.join(", ")
            )));
        }
        if self.seconds == 0 {
            return Err(CampaignError::Refused(
                "a worker's budget must be at least 1 second".to_owned(),
            ));
        }
        if let Some(mode) = &self.feedback_mode {
            if mode != "on" && mode != "off" {
                return Err(CampaignError::Refused(format!(
                    "{FEEDBACK_VAR} must be `on` or `off`, not `{mode}`"
                )));
            }
        }
        for arg in &self.libfuzzer_args {
            let name = arg.split_once('=').map_or(arg.as_str(), |(name, _)| name);
            if !arg.starts_with('-') || RESERVED_FLAGS.contains(&name) {
                return Err(CampaignError::Refused(format!(
                    "`{arg}` can't be an extra libFuzzer flag: the supervisor sets the \
                     corpus, {}",
                    RESERVED_FLAGS.join(", ")
                )));
            }
        }
        Ok(())
    }
}

/// One worker.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerPlan {
    /// The worker's number, from 0.
    pub index: usize,
    /// libFuzzer's `-seed` for the worker's first process. Its `n`th
    /// restart adds `n`, wrapping.
    pub seed: u32,
    /// The CPUs the worker's processes run on, as `taskset -c` takes them.
    /// `None` leaves placement to the kernel.
    pub cpus: Option<String>,
}

/// The curated seeds in a segment's first snapshot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SeedsRecord {
    /// The fixture directories they came from.
    pub fixtures: Vec<PathBuf>,
    /// Each admitted seed's full fuzz input, by digest, in order.
    pub admitted: Vec<Digest>,
    /// How many optional seeds failed validation and stayed out.
    pub quarantined: usize,
}

/// What `start` needs.
#[derive(Clone, Debug)]
pub struct StartOptions {
    /// The campaign's name.
    pub campaign: String,
    /// The harness the fuzz target runs.
    pub harness: &'static HarnessEntry,
    /// The fuzzer's build, from [`Store::save_build`].
    pub build: Digest,
    /// The workers.
    pub workers: Vec<WorkerPlan>,
    /// Each worker's budget.
    pub budget: Budget,
    /// The fixture directories to take curated seeds from.
    pub fixtures: Vec<PathBuf>,
    /// The full-suite manifest the run belongs to, if any.
    pub suite: Option<PathBuf>,
    /// Where the operator copies the store.
    pub artifacts: Option<String>,
    /// For a new segment: the previous segment's last run, and how to
    /// carry its corpus.
    pub parent: Option<(Digest, Migration)>,
}

/// What `resume` changes. Everything else comes from the run it resumes.
#[derive(Clone, Debug, Default)]
pub struct ResumeOptions {
    /// The run to resume. `None` resumes the store's only run that no
    /// other run continues.
    pub parent: Option<Digest>,
    /// The fuzzer build. `None` keeps the parent's. Another build must
    /// have the parent build's identity, such as the same commit built on
    /// another host, and both builds must be clean.
    pub build: Option<Digest>,
    /// Each worker's budget in seconds. `None` keeps the parent's.
    pub seconds: Option<u64>,
    /// The number of workers. `None` keeps the parent's.
    pub workers: Option<usize>,
}

impl Store {
    /// Reads manifest `digest`, and checks its digest and version.
    pub fn read_manifest(&self, digest: &Digest) -> Result<Manifest, CampaignError> {
        let path = self.manifest_path(digest);
        let manifest: Manifest = read_record(&path, Some(digest))?;
        if manifest.version != STORE_VERSION {
            return Err(CampaignError::corrupt(
                &path,
                format!(
                    "it's at version {}, and this build reads {STORE_VERSION}",
                    manifest.version
                ),
            ));
        }
        Ok(manifest)
    }

    /// Every manifest in the store, by digest.
    pub fn manifests(&self) -> Result<BTreeMap<Digest, Manifest>, CampaignError> {
        let dir = self.manifests_dir();
        let entries =
            fs::read_dir(&dir).map_err(CampaignError::io(format!("listing {}", dir.display())))?;
        let mut manifests = BTreeMap::new();
        for entry in entries {
            let entry = entry.map_err(CampaignError::io(format!("listing {}", dir.display())))?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with('.') {
                continue;
            }
            let digest = name
                .strip_suffix(".json")
                .and_then(|stem| stem.parse::<Digest>().ok())
                .ok_or_else(|| {
                    CampaignError::corrupt(&entry.path(), "it isn't named `<sha256>.json`")
                })?;
            let manifest = self.read_manifest(&digest)?;
            manifests.insert(digest, manifest);
        }
        Ok(manifests)
    }

    /// The runs no other run continues, by resume or by a new segment.
    pub fn leaves(&self) -> Result<Vec<Digest>, CampaignError> {
        let manifests = self.manifests()?;
        let continued: BTreeSet<&Digest> = manifests
            .values()
            .flat_map(|manifest| [&manifest.resume_parent, &manifest.previous_segment])
            .flatten()
            .collect();
        Ok(manifests
            .keys()
            .filter(|digest| !continued.contains(digest))
            .cloned()
            .collect())
    }

    /// The store's only leaf. Refuses if it has none or several.
    fn only_leaf(&self) -> Result<Digest, CampaignError> {
        match self.leaves()?.as_slice() {
            [leaf] => Ok(leaf.clone()),
            [] => Err(CampaignError::Refused(format!(
                "{} holds no run",
                self.root().display()
            ))),
            leaves => Err(CampaignError::Refused(format!(
                "{} holds {} runs that no run continues; name one with --run: {}",
                self.root().display(),
                leaves.len(),
                leaves
                    .iter()
                    .map(Digest::as_str)
                    .collect::<Vec<_>>()
                    .join(", ")
            ))),
        }
    }

    /// Writes `manifest` under its digest, and returns the digest.
    fn write_manifest(&self, manifest: &Manifest) -> Result<Digest, CampaignError> {
        let bytes = record_bytes(manifest);
        let digest = Digest::of(&bytes);
        let path = self.manifest_path(&digest);
        write_once(&path, &bytes)
            .map_err(CampaignError::io(format!("writing {}", path.display())))?;
        Ok(digest)
    }

    /// Ends `parent` if it's open, by [`close`], and returns its end
    /// record's snapshot to start from, after checking that snapshot and
    /// the record against the run's files.
    fn ended_head(&self, parent: &Digest) -> Result<Digest, CampaignError> {
        match self.run_state(parent)? {
            RunState::Running => {
                return Err(CampaignError::Refused(format!(
                    "run {parent} is still going"
                )))
            }
            RunState::Open => {
                close(self, parent)?;
            }
            RunState::Ended => {}
        }
        let end = read_end(self, parent)?;
        verify_end(self, parent, &self.read_manifest(parent)?, &end)?;
        self.verify_snapshot(end.head())?;
        Ok(end.head().clone())
    }
}

/// Writes the manifest of a campaign segment's first run, and returns the
/// run locked for [`Prepared::run`].
///
/// The run's initial snapshot holds `options.harness`'s curated seeds from
/// `options.fixtures`, which must all validate, and with
/// [`Migration::Carry`] the previous segment's last snapshot too, which
/// needs clean fuzzer builds in both segments. A store with a run must name
/// it as `options.parent`, since a store holds one campaign.
pub fn prepare_start(store: &Store, options: StartOptions) -> Result<Prepared, CampaignError> {
    options.budget.check()?;
    if options.workers.is_empty() {
        return Err(CampaignError::Refused(
            "a run needs at least one worker".to_owned(),
        ));
    }
    let _lock = store.lock_store()?;
    let build = store.read_build(&options.build)?;
    let runner = BuildInfo::current();
    check_runner(&runner, &build)?;
    check_harness(&build, options.harness.name)?;
    let harness = HarnessRecord::of(options.harness)?;
    let (segment, previous) = match &options.parent {
        None => {
            if !store.manifests()?.is_empty() {
                return Err(CampaignError::Refused(format!(
                    "{} already holds a campaign: resume it, or start a new segment with \
                     --parent",
                    store.root().display()
                )));
            }
            (1, None)
        }
        Some((parent, migration)) => {
            let manifest = store.read_manifest(parent)?;
            if !store.leaves()?.contains(parent) {
                return Err(CampaignError::Refused(format!(
                    "run {parent} is already continued; a new segment follows the last run"
                )));
            }
            if *migration == Migration::Carry {
                check_carry(&manifest.build, &build)?;
            }
            let head = store.ended_head(parent)?;
            if *migration == Migration::Carry && manifest.harness.versions != harness.versions {
                return Err(CampaignError::Refused(format!(
                    "the harness's versions changed from {:?} to {:?}, so the previous \
                     segment's inputs can mean other scenarios; migrate with --corpus seeds",
                    manifest.harness.versions, harness.versions
                )));
            }
            (
                manifest.segment + 1,
                Some((parent.clone(), *migration, head)),
            )
        }
    };
    let report = seed::load(&options.fixtures, Some(options.harness.name))
        .map_err(CampaignError::io("listing the fixtures"))?;
    if !report.rejected.is_empty() {
        let reasons: Vec<String> = report
            .rejected
            .iter()
            .map(|failed| format!("{}: {}", failed.path.display(), failed.error))
            .collect();
        return Err(CampaignError::Refused(format!(
            "required seeds failed validation: {}",
            reasons.join("; ")
        )));
    }
    let mut sources: Vec<Source<'_>> = report
        .admitted
        .iter()
        .map(|seed| Source::Input(&seed.input))
        .collect();
    let carried = previous
        .as_ref()
        .filter(|(_, migration, _)| *migration == Migration::Carry)
        .map(|(_, _, head)| store.snapshot_inputs(head));
    if let Some(inputs) = &carried {
        sources.push(Source::Dir(inputs));
    }
    let snapshot = store.save_snapshot(&sources)?;
    let feedback = probe_feedback(store, &build, &options.budget)?;
    let suite_sha256 = options
        .suite
        .as_deref()
        .map(|path| {
            Digest::of_file(path).map_err(CampaignError::io(format!("reading {}", path.display())))
        })
        .transpose()?;
    let manifest = Manifest {
        version: STORE_VERSION,
        campaign: options.campaign,
        segment,
        previous_segment: previous.as_ref().map(|(parent, _, _)| parent.clone()),
        migration: previous.as_ref().map(|(_, migration, _)| *migration),
        resume_parent: None,
        created_unix: now_unix(),
        build,
        runner,
        harness,
        feedback,
        suite_sha256,
        budget: options.budget,
        workers: options.workers,
        initial_snapshot: snapshot.digest,
        seeds: SeedsRecord {
            fixtures: options.fixtures,
            admitted: report
                .admitted
                .iter()
                .map(|seed| Digest::of(&seed.input))
                .collect(),
            quarantined: report.quarantined.len(),
        },
        artifacts: options.artifacts,
    };
    let digest = store.write_manifest(&manifest)?;
    Prepared::lock(store, digest)
}

/// Writes the manifest of a run that resumes another, and returns the run
/// locked for [`Prepared::run`].
///
/// An open parent is ended first, by [`close`]. The new run starts from the
/// parent's end snapshot, after checking that snapshot's inputs and every
/// failure file the parent's end record lists. It refuses unless the
/// fuzzer build has the parent's identity, the runner comes from the
/// build's commit, and the harness and feedback match the parent's: a run
/// that differs in any of them starts a new segment instead. Another binary
/// of the same identity resumes only if both builds are
/// [clean](super::BuildIdentity::clean).
pub fn prepare_resume(store: &Store, options: ResumeOptions) -> Result<Prepared, CampaignError> {
    let _lock = store.lock_store()?;
    let parent_digest = match options.parent {
        Some(parent) => parent,
        None => store.only_leaf()?,
    };
    if !store.leaves()?.contains(&parent_digest) {
        return Err(CampaignError::Refused(format!(
            "run {parent_digest} is already continued; resume the last run"
        )));
    }
    let parent = store.read_manifest(&parent_digest)?;
    let head = store.ended_head(&parent_digest)?;
    let build = store.read_build(options.build.as_ref().unwrap_or(&parent.build.binary))?;
    let mut differences: Vec<String> = parent
        .build
        .identity
        .differences(&build.identity)
        .into_iter()
        .map(|field| format!("build {field}"))
        .collect();
    // Two binaries share a resumable identity only if both builds' identities
    // say what they were built from.
    if build.binary != parent.build.binary
        && !(parent.build.identity.clean() && build.identity.clean())
    {
        differences.push("build binary, which a dirty or unknown build must keep".to_owned());
    }
    let runner = BuildInfo::current();
    check_runner(&runner, &build)?;
    check_harness(&build, &parent.harness.name)?;
    let entry = crate::harness::find(&parent.harness.name)
        .map_err(|error| CampaignError::Refused(error.to_string()))?;
    let harness = HarnessRecord::of(entry)?;
    if harness != parent.harness {
        differences.push("harness".to_owned());
    }
    let refuse = |differences: &[String]| {
        if differences.is_empty() {
            return Ok(());
        }
        Err(CampaignError::Refused(format!(
            "run {parent_digest} can't be resumed with this build: {} differ. Start a new \
             segment with `campaign start --parent {parent_digest} --corpus carry|seeds`",
            differences.join(", ")
        )))
    };
    // The probe runs the fuzzer, so only a build that may resume gets that far.
    refuse(&differences)?;
    let budget = Budget {
        seconds: options.seconds.unwrap_or(parent.budget.seconds),
        ..parent.budget.clone()
    };
    budget.check()?;
    let feedback = probe_feedback(store, &build, &budget)?;
    if feedback != parent.feedback {
        differences.push("feedback".to_owned());
    }
    refuse(&differences)?;
    let count = options.workers.unwrap_or(parent.workers.len());
    if count == 0 {
        return Err(CampaignError::Refused(
            "a run needs at least one worker".to_owned(),
        ));
    }
    let workers = (0..count)
        .map(|index| WorkerPlan {
            index,
            seed: resumed_seed(&parent_digest, index),
            cpus: parent.workers.get(index).and_then(|plan| plan.cpus.clone()),
        })
        .collect();
    let manifest = Manifest {
        resume_parent: Some(parent_digest),
        created_unix: now_unix(),
        build,
        runner,
        feedback,
        budget,
        workers,
        initial_snapshot: head,
        ..parent
    };
    let digest = store.write_manifest(&manifest)?;
    Prepared::lock(store, digest)
}

/// The libFuzzer seed of worker `index` in a run that resumes `parent`: the
/// first four bytes of the SHA-256 of `<parent>/<index>`. A resumed worker
/// then doesn't repeat its parent's sequence.
fn resumed_seed(parent: &Digest, index: usize) -> u32 {
    let digest = Digest::of(format!("{parent}/{index}").as_bytes());
    u32::from_str_radix(&digest.as_str()[..8], 16).expect("a digest's first 8 hex digits are a u32")
}

/// Refuses a runner from another commit than the fuzzer's build, or a
/// build from an unknown commit, since the runner's harness registry might
/// not be the one the fuzzer runs.
fn check_runner(runner: &BuildInfo, build: &FuzzBuild) -> Result<(), CampaignError> {
    if build.identity.git == UNKNOWN {
        return Err(CampaignError::Refused(format!(
            "fuzzer {} comes from an unknown commit, so no zakura-sim can be shown to share \
             its harness registry; build it in a git checkout",
            build.binary
        )));
    }
    if runner.git != build.identity.git {
        return Err(CampaignError::Refused(format!(
            "this zakura-sim comes from commit {}, and fuzzer {} from commit {}; build both \
             from one checkout",
            runner.git, build.binary, build.identity.git
        )));
    }
    Ok(())
}

/// Refuses to carry a corpus from the fuzzer `previous` to the fuzzer
/// `next` unless both builds are [clean](super::BuildIdentity::clean).
/// Carry trusts the harness's versions, from the runner's registry, to say
/// whether an input keeps its meaning. A dirty fuzzer can run a generator
/// change that no version records.
fn check_carry(previous: &FuzzBuild, next: &FuzzBuild) -> Result<(), CampaignError> {
    for build in [previous, next] {
        if !build.identity.clean() {
            return Err(CampaignError::Refused(format!(
                "fuzzer {} is dirty or of unknown provenance, so the harness's versions may \
                 not say what its generator does; carry a corpus only between clean builds, \
                 or migrate with --corpus seeds",
                build.binary
            )));
        }
    }
    Ok(())
}

/// Refuses a fuzzer whose target runs another harness than `harness`, so a
/// manifest never names a harness its fuzzer doesn't run.
fn check_harness(build: &FuzzBuild, harness: &str) -> Result<(), CampaignError> {
    if build.identity.harness != harness {
        return Err(CampaignError::Refused(format!(
            "fuzzer {} is target {}, which runs harness {}, not {harness}",
            build.binary, build.identity.target, build.identity.harness
        )));
    }
    Ok(())
}

/// Runs `build`'s fuzzer once with `-runs=0` and no corpus, and returns the
/// feedback it logs. Fails if the fuzzer fails, so a manifest never names
/// a fuzzer that can't start.
fn probe_feedback(
    store: &Store,
    build: &FuzzBuild,
    budget: &Budget,
) -> Result<Option<FeedbackRecord>, CampaignError> {
    let dir = partial_path(store.root());
    let probed = probe_in(store, build, budget, &dir);
    // The probe's result matters more than a failed cleanup.
    let _ = fs::remove_dir_all(&dir);
    probed
}

/// [`probe_feedback`] with its work in `dir`.
fn probe_in(
    store: &Store,
    build: &FuzzBuild,
    budget: &Budget,
    dir: &Path,
) -> Result<Option<FeedbackRecord>, CampaignError> {
    let corpus = dir.join("corpus");
    fs::create_dir_all(&corpus)
        .map_err(CampaignError::io(format!("creating {}", corpus.display())))?;
    let mut command = fuzzer_command(
        &store.fuzzer_path(&build.binary),
        None,
        budget,
        &dir.join("sim"),
    );
    command.arg("-runs=0").arg(&corpus);
    let (exit, log) = run_logged(
        command,
        &dir.join("log.txt"),
        Some(PROBE_TIMEOUT),
        None,
        |_| {},
    )?;
    let log = String::from_utf8_lossy(&log);
    if let Some(failure) = ProcessFailure::of(exit, log.as_bytes()) {
        let tail: Vec<&str> = log.lines().rev().take(20).collect();
        return Err(CampaignError::Refused(format!(
            "fuzzer {} failed its startup probe with {failure:?}:\n{}",
            build.binary,
            tail.into_iter().rev().collect::<Vec<_>>().join("\n")
        )));
    }
    FeedbackRecord::parse(&log).map_err(CampaignError::Refused)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The feedback line parses to its fields, and a log without one has
    /// none.
    #[test]
    fn feedback_lines_parse() {
        let log = "INFO: Seed: 1\nzakura-sim feedback: backend=libfuzzer-extra-counters \
                   version=1 features=65536 mode=on\nINFO: Loaded 1 modules\n";
        let record = FeedbackRecord::parse(log).unwrap().unwrap();
        assert_eq!(record.backend, "libfuzzer-extra-counters");
        assert_eq!(record.version, "1");
        assert_eq!(record.mode, "on");
        assert_eq!(record.logged["features"], "65536");
        assert_eq!(FeedbackRecord::parse("INFO: Seed: 1\n"), Ok(None));
        assert!(FeedbackRecord::parse("zakura-sim feedback: backend=x mode=on\n").is_err());
    }

    /// Extra flags can't set what the supervisor sets.
    #[test]
    fn reserved_flags_are_refused() {
        let budget = |args: &[&str]| Budget {
            seconds: 1,
            max_restarts: 0,
            max_len: 4096,
            timeout_secs: 30,
            rss_limit_mb: 4096,
            abandon_limit: 1,
            feedback_mode: None,
            sanitizer_options: BTreeMap::new(),
            compact_secs: None,
            libfuzzer_args: args.iter().map(|arg| (*arg).to_owned()).collect(),
        };
        assert!(budget(&["-use_value_profile=1"]).check().is_ok());
        assert!(budget(&["-seed=3"]).check().is_err());
        assert!(budget(&["-runs=10"]).check().is_err());
        assert!(budget(&["corpus"]).check().is_err());
    }
}
