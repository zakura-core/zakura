//! What a fuzz process's runs came to, counted per process and written to a
//! file its campaign reads (explorer implementation plan, E4b-min).
//!
//! A fuzz target with [`STATS_VAR`] set keeps a [`Recorder`]. The recorder
//! counts each run's outcome, end, cleanup, property reports and features
//! in [`Counts`], and writes them as a [`ProcessStats`] to
//! `$ZAKURA_SIM_STATS/stats-<pid>.json`: every [`CHECKPOINT`], when the
//! target stops the process on a run it counted, and once more at a clean
//! exit. Only the clean exit marks the file complete. A process that
//! crashes, or that its supervisor kills, leaves the counts of its last
//! checkpoint, and the file says it's incomplete.
//!
//! A campaign gives each worker its own stats directory, and
//! `zakura-sim campaign evidence` sums a run's files.

use std::{
    collections::BTreeMap,
    fs, io,
    path::{Path, PathBuf},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};

use super::{
    feedback::Feature,
    state::{Family, SPAN},
    Classified, ExploreOutcome, ExploreRun,
};
use crate::{
    check::PropertyOutcome,
    end::Outcome,
    harness::HarnessEntry,
    property::{Inconclusive, Status},
    record::OutcomeKind,
};

/// The variable that names the directory a fuzz process writes its stats
/// file to. Unset or empty, the process keeps no stats.
pub const STATS_VAR: &str = "ZAKURA_SIM_STATS";

/// The version of [`ProcessStats`]. Bump it when a field changes meaning.
pub const STATS_VERSION: u32 = 1;

/// How often a recorder rewrites its file while its process runs.
pub const CHECKPOINT: Duration = Duration::from_secs(10);

/// What the target did with a run: [`ExploreOutcome`]'s kind.
pub const OUTCOMES: &[&str] = &["clean", "fail", "harness-failure", "abandoned"];

/// The name [`Counts::ends`] gives a run whose harness couldn't run its
/// scenario, so the run has no end.
pub const NOT_RUN: &str = "not-run";

/// One process's counts, or the sum of several.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Counts {
    /// Inputs run.
    pub runs: u64,
    /// Runs by what the target did with them: each of [`OUTCOMES`].
    pub outcomes: BTreeMap<String, u64>,
    /// Runs by how they ended, as the `end` record names it: `violation`,
    /// `budget`, `idle/<status>` or `harness`, and [`NOT_RUN`] for a
    /// scenario the harness couldn't run.
    pub ends: BTreeMap<String, u64>,
    /// Runs whose drain after the step budget stopped at its bound with
    /// tasks still ready.
    pub drains_exhausted: u64,
    /// What shutdown did.
    pub cleanup: CleanupCounts,
    /// Each property's counts, by ID: every property the harness
    /// registers, and any other property a run reported.
    pub properties: BTreeMap<String, PropertyCounts>,
    /// How many runs reached each feature, by index. Empty for a target
    /// without a collector.
    pub features: BTreeMap<u16, u64>,
}

/// What shutdown did, in runs.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CleanupCounts {
    /// Runs whose shutdown abandoned the run and leaked its tasks.
    pub abandoned: u64,
    /// Tasks those runs leaked.
    pub leaked_tasks: u64,
    /// Runs whose shutdown force-unwound a parked task.
    pub unwound: u64,
    /// Runs with a task that panicked during cleanup.
    pub panicked: u64,
    /// Runs with a task that caught a forced unwind.
    pub unwind_failures: u64,
}

/// One property's outcomes, in runs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PropertyCounts {
    /// Its status in the harness. A property the harness doesn't register
    /// is `Active`, as the classifier counts it.
    pub status: Status,
    /// Runs where it held.
    pub held: u64,
    /// The evaluations that applied in those runs.
    pub evaluations: u64,
    /// Runs where no evaluation applied.
    pub not_applicable: u64,
    /// Runs where it was inconclusive, by reason.
    pub inconclusive: BTreeMap<String, u64>,
    /// Runs where it was violated.
    pub violated: u64,
}

impl PropertyCounts {
    /// Zero counts at `status`.
    pub fn new(status: Status) -> Self {
        PropertyCounts {
            status,
            held: 0,
            evaluations: 0,
            not_applicable: 0,
            inconclusive: BTreeMap::new(),
            violated: 0,
        }
    }

    /// True if an evaluation applied in some run: it held with an
    /// applicable evaluation, or it was violated.
    pub fn applied(&self) -> bool {
        self.evaluations > 0 || self.violated > 0
    }

    /// Runs where its assumptions failed: they never all held, or one
    /// stopped holding while an obligation was open.
    pub fn assumption_losses(&self) -> u64 {
        [
            Inconclusive::AssumptionsNeverHeld,
            Inconclusive::AssumptionLost,
        ]
        .iter()
        .map(|reason| {
            self.inconclusive
                .get(&reason.to_string())
                .copied()
                .unwrap_or(0)
        })
        .sum()
    }

    /// Adds `other`'s counts. Keeps this status.
    fn add(&mut self, other: &PropertyCounts) {
        self.held += other.held;
        self.evaluations += other.evaluations;
        self.not_applicable += other.not_applicable;
        add_map(&mut self.inconclusive, &other.inconclusive);
        self.violated += other.violated;
    }
}

impl Counts {
    /// Zero counts, with every property `entry` registers at its status.
    pub fn for_harness(entry: &HarnessEntry) -> Self {
        Counts {
            properties: (entry.properties)()
                .into_iter()
                .map(|property| (property.id, PropertyCounts::new(property.status)))
                .collect(),
            ..Counts::default()
        }
    }

    /// Counts one run, its class, and the features it reached.
    pub fn record(&mut self, run: &ExploreRun, classified: &Classified, features: &[Feature]) {
        self.runs += 1;
        let outcome = match classified.outcome {
            ExploreOutcome::Clean => "clean",
            ExploreOutcome::Fail { .. } => "fail",
            ExploreOutcome::HarnessFailure { .. } => "harness-failure",
            ExploreOutcome::Abandoned { .. } => "abandoned",
        };
        bump(&mut self.outcomes, outcome);
        for feature in features {
            *self.features.entry(feature.index()).or_default() += 1;
        }
        let Some(report) = run.report() else {
            bump(&mut self.ends, NOT_RUN);
            return;
        };
        // The `end` record's names, read from the report itself, since the
        // record clones every property report.
        let name = match report.end.outcome {
            Outcome::Violation => serialized_name(&OutcomeKind::Violation),
            Outcome::Budget => serialized_name(&OutcomeKind::Budget),
            Outcome::Idle { status } => format!("idle/{}", serialized_name(&status)),
            Outcome::Harness => serialized_name(&OutcomeKind::Harness),
        };
        bump(&mut self.ends, &name);
        self.drains_exhausted += u64::from(report.end.drain.is_some_and(|drain| drain.exhausted));
        let (cleanup, report_cleanup) = (&mut self.cleanup, &report.cleanup);
        cleanup.abandoned += u64::from(report_cleanup.leaked > 0);
        cleanup.leaked_tasks += count(report_cleanup.leaked);
        cleanup.unwound += u64::from(report_cleanup.unwound > 0);
        cleanup.panicked += u64::from(report_cleanup.panicked > 0);
        cleanup.unwind_failures += u64::from(report_cleanup.unwind_failures > 0);
        for property in &report.properties {
            // A lookup first, so a known property costs no allocation.
            if !self.properties.contains_key(&property.property) {
                self.properties.insert(
                    property.property.clone(),
                    PropertyCounts::new(Status::Active),
                );
            }
            let counts = self
                .properties
                .get_mut(&property.property)
                .expect("the property was just inserted");
            match &property.outcome {
                PropertyOutcome::Held { applicable } => {
                    counts.held += 1;
                    counts.evaluations += u64::from(*applicable);
                }
                PropertyOutcome::NotApplicable => counts.not_applicable += 1,
                PropertyOutcome::Inconclusive { reason } => bump(&mut counts.inconclusive, reason),
                PropertyOutcome::Violated { .. } => counts.violated += 1,
            }
        }
    }

    /// Adds `other`'s counts. A property only `other` has keeps its status.
    pub fn add(&mut self, other: &Counts) {
        self.runs += other.runs;
        add_map(&mut self.outcomes, &other.outcomes);
        add_map(&mut self.ends, &other.ends);
        self.drains_exhausted += other.drains_exhausted;
        let (cleanup, more) = (&mut self.cleanup, &other.cleanup);
        cleanup.abandoned += more.abandoned;
        cleanup.leaked_tasks += more.leaked_tasks;
        cleanup.unwound += more.unwound;
        cleanup.panicked += more.panicked;
        cleanup.unwind_failures += more.unwind_failures;
        for (id, counts) in &other.properties {
            match self.properties.get_mut(id) {
                Some(mine) => mine.add(counts),
                None => {
                    self.properties.insert(id.clone(), counts.clone());
                }
            }
        }
        add_map(&mut self.features, &other.features);
    }

    /// Runs that ended at the step budget.
    pub fn budget_exits(&self) -> u64 {
        self.ends
            .get(&serialized_name(&OutcomeKind::Budget))
            .copied()
            .unwrap_or(0)
    }

    /// How many distinct features the runs reached, in all and by family:
    /// a [`Family`]'s name for a protocol-state feature, and `other` for any
    /// other feature, such as a probe's.
    pub fn feature_totals(&self) -> (usize, BTreeMap<&'static str, usize>) {
        let mut families = BTreeMap::new();
        for index in self.features.keys() {
            let family = Family::ALL
                .get(usize::from(index / SPAN))
                .map_or("other", |family| family.name());
            *families.entry(family).or_default() += 1;
        }
        (self.features.len(), families)
    }
}

/// One process's stats file.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessStats {
    /// [`STATS_VERSION`].
    pub version: u32,
    /// The harness the process ran.
    pub harness: String,
    /// The process's ID.
    pub pid: u32,
    /// When the process made its recorder, in seconds since the Unix epoch.
    pub started_unix: u64,
    /// When the process wrote this file, in seconds since the Unix epoch.
    pub written_unix: u64,
    /// True only once the process exited cleanly and wrote its final
    /// counts. False while it runs, and for good if it crashed or was
    /// killed: the runs since its last checkpoint are then missing.
    pub complete: bool,
    /// The counts.
    pub counts: Counts,
}

impl ProcessStats {
    /// The stats file of process `pid` in `dir`.
    pub fn path(dir: &Path, pid: u32) -> PathBuf {
        dir.join(format!("stats-{pid}.json"))
    }

    /// The process ID a stats file's name holds, if it's one, exactly as
    /// [`ProcessStats::path`] writes it.
    pub fn pid_of(name: &str) -> Option<u32> {
        let digits = name.strip_prefix("stats-")?.strip_suffix(".json")?;
        let pid: u32 = digits.parse().ok()?;
        (pid.to_string() == digits).then_some(pid)
    }

    /// Reads the stats file at `path`, at [`STATS_VERSION`].
    pub fn read(path: &Path) -> Result<ProcessStats, String> {
        let bytes =
            fs::read(path).map_err(|error| format!("reading {}: {error}", path.display()))?;
        let stats: ProcessStats = serde_json::from_slice(&bytes)
            .map_err(|error| format!("parsing {}: {error}", path.display()))?;
        if stats.version != STATS_VERSION {
            return Err(format!(
                "{} is at stats version {}, and this build reads {STATS_VERSION}",
                path.display(),
                stats.version
            ));
        }
        Ok(stats)
    }
}

/// A fuzz process's counts and the file it writes them to.
#[derive(Debug)]
pub struct Recorder {
    /// The file.
    path: PathBuf,
    /// What the file holds at the next write.
    stats: ProcessStats,
    /// When the recorder last wrote the file.
    written: Instant,
    /// True from [`Recorder::begin`] until the run is counted.
    running: bool,
}

impl Recorder {
    /// A recorder for process `pid` on `entry`'s harness, which writes to
    /// its stats file in `dir`. Creates `dir`, and writes the file with no
    /// runs, so a process that crashes on its first input still leaves one.
    pub fn new(dir: &Path, entry: &HarnessEntry, pid: u32) -> io::Result<Recorder> {
        fs::create_dir_all(dir)?;
        let now = now_unix();
        let mut recorder = Recorder {
            path: ProcessStats::path(dir, pid),
            stats: ProcessStats {
                version: STATS_VERSION,
                harness: entry.name.to_owned(),
                pid,
                started_unix: now,
                written_unix: now,
                complete: false,
                counts: Counts::for_harness(entry),
            },
            written: Instant::now(),
            running: false,
        };
        recorder.write()?;
        Ok(recorder)
    }

    /// The file it writes.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The counts so far.
    pub fn counts(&self) -> &Counts {
        &self.stats.counts
    }

    /// Marks a run in progress until [`Recorder::record`] counts it.
    pub fn begin(&mut self) {
        self.running = true;
    }

    /// Counts one run, as [`Counts::record`] does.
    pub fn record(&mut self, run: &ExploreRun, classified: &Classified, features: &[Feature]) {
        self.stats.counts.record(run, classified, features);
        self.running = false;
    }

    /// Writes the counts if [`CHECKPOINT`] has passed since the last write.
    pub fn checkpoint_if_due(&mut self) -> io::Result<()> {
        if self.written.elapsed() >= CHECKPOINT {
            self.checkpoint()?;
        }
        Ok(())
    }

    /// Writes the counts, still incomplete.
    pub fn checkpoint(&mut self) -> io::Result<()> {
        self.write()
    }

    /// Writes the final counts, marked complete unless a run is in
    /// progress: a process that exits inside a run leaves that run
    /// uncounted. A process calls it once, as it exits.
    pub fn finish(&mut self) -> io::Result<()> {
        self.stats.complete = !self.running;
        self.write()
    }

    /// Writes the file to a temporary name, then renames it into place, so
    /// a reader never sees a partial file.
    fn write(&mut self) -> io::Result<()> {
        self.stats.written_unix = now_unix();
        let bytes = serde_json::to_vec_pretty(&self.stats).map_err(io::Error::other)?;
        let partial = self.path.with_extension("json.partial");
        fs::write(&partial, bytes)?;
        fs::rename(&partial, &self.path)?;
        self.written = Instant::now();
        Ok(())
    }
}

/// Adds 1 to `key`'s count, allocating its key only the first time.
fn bump(map: &mut BTreeMap<String, u64>, key: &str) {
    match map.get_mut(key) {
        Some(count) => *count += 1,
        None => {
            map.insert(key.to_owned(), 1);
        }
    }
}

/// Adds each of `more`'s counts to `into`'s.
fn add_map<K: Ord + Clone>(into: &mut BTreeMap<K, u64>, more: &BTreeMap<K, u64>) {
    for (key, count) in more {
        *into.entry(key.clone()).or_default() += count;
    }
}

/// `value`'s name as it serializes: a unit variant's kebab-case name.
fn serialized_name(value: &impl Serialize) -> String {
    match serde_json::to_value(value) {
        Ok(serde_json::Value::String(name)) => name,
        other => unreachable!("a unit variant serializes to its name, not {other:?}"),
    }
}

/// `n` as a count.
fn count(n: usize) -> u64 {
    // A usize always fits in a u64 on supported targets.
    n as u64
}

/// Seconds since the Unix epoch.
fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_secs())
}
