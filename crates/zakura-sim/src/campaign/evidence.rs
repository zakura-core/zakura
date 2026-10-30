//! A run's evidence: what its workers' fuzzer processes counted, summed per
//! worker and for the run, with every way the counts fall short
//! (explorer implementation plan, E4b-min).
//!
//! Each worker's processes write their stats files to the worker's
//! `stats/` directory ([`crate::explore::stats`]). The evidence matches the
//! files to the worker's attempts log by process ID, and lists each gap as
//! an [`Incomplete`]: a worker that didn't spend its budget, a process that
//! left no file, a file whose process didn't exit cleanly, and a file the
//! log doesn't list. Only complete files count toward
//! [`Evidence::complete_totals`], and so toward [`Evidence::unapplied`].

use std::{
    collections::{BTreeMap, BTreeSet},
    fs, io,
    path::Path,
};

use serde::Serialize;

use super::{
    run::{
        read_attempts, read_end, worker_dir, AttemptEnd, WorkerOutcome, OUTCOME_FILE, STATS_DIR,
    },
    store::{read_record, CampaignError, Digest, RunState, Store},
    Status, WorkerEnd,
};
use crate::explore::stats::{Counts, ProcessStats};

/// A run's evidence.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Evidence {
    /// The run's manifest.
    pub run: Digest,
    /// The harness it fuzzed.
    pub harness: String,
    /// How the run ended, or `None` while it has no end record.
    pub status: Option<Status>,
    /// Each worker's evidence, by index.
    pub workers: Vec<WorkerEvidence>,
    /// Every stats file's counts, summed.
    pub totals: Counts,
    /// The complete stats files' counts, summed.
    pub complete_totals: Counts,
    /// The properties, of the harness's and of any stats file, that no
    /// complete stats file saw apply: none of their runs held with an
    /// applicable evaluation or violated them.
    pub unapplied: Vec<String>,
    /// Every way the counts fall short, by worker.
    pub incomplete: Vec<Incomplete>,
}

/// One worker's evidence.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct WorkerEvidence {
    /// The worker's number.
    pub index: usize,
    /// Why it stopped. `None` while it has no outcome.
    pub outcome: Option<WorkerOutcome>,
    /// Its fuzzer processes by how they ended.
    pub attempts: BTreeMap<AttemptEnd, u64>,
    /// How many stats files it holds.
    pub processes: usize,
    /// Their counts, summed.
    pub counts: Counts,
}

/// One way a run's counts fall short.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(tag = "reason", rename_all = "kebab-case")]
pub enum Incomplete {
    /// The worker has no outcome, or stopped with its supervisor: its
    /// budget isn't spent.
    Unfinished {
        /// The worker.
        worker: usize,
    },
    /// The worker stopped at its restart limit before its budget was
    /// spent.
    RestartLimit {
        /// The worker.
        worker: usize,
    },
    /// A process the attempts log lists left no stats file.
    MissingStats {
        /// The worker.
        worker: usize,
        /// The attempt.
        attempt: u32,
        /// Its process ID.
        pid: u32,
    },
    /// A process didn't exit cleanly, or still runs: its stats file lacks
    /// the runs since its last checkpoint.
    PartialStats {
        /// The worker.
        worker: usize,
        /// Its process ID.
        pid: u32,
    },
    /// A stats file whose process the attempts log doesn't list: the
    /// supervisor stopped while the process ran.
    UnloggedProcess {
        /// The worker.
        worker: usize,
        /// Its process ID.
        pid: u32,
    },
    /// A stats file that can't be read, or that another harness wrote. Its
    /// counts are left out.
    UnreadableStats {
        /// The worker.
        worker: usize,
        /// The file's name.
        file: String,
        /// Why.
        error: String,
    },
}

/// Reads run `digest`'s evidence from its records and its workers' stats
/// files. The run may still be running.
pub fn evidence(store: &Store, digest: &Digest) -> Result<Evidence, CampaignError> {
    let manifest = store.read_manifest(digest)?;
    let end = match store.run_state(digest)? {
        RunState::Ended => Some(read_end(store, digest)?),
        RunState::Running | RunState::Open => None,
    };
    let dir = store.run_dir(digest);
    let mut evidence = Evidence {
        run: digest.clone(),
        harness: manifest.harness.name.clone(),
        status: end.as_ref().map(|end| end.status),
        workers: Vec::new(),
        totals: Counts::default(),
        complete_totals: Counts::default(),
        unapplied: Vec::new(),
        incomplete: Vec::new(),
    };
    for plan in &manifest.workers {
        let worker = worker_dir(&dir, plan.index);
        let outcome = match end.as_ref() {
            Some(end) => end
                .workers
                .iter()
                .find(|ended| ended.index == plan.index)
                .map(|ended| ended.outcome),
            None => outcome_record(&worker)?.map(|ended| ended.outcome),
        };
        match outcome {
            Some(WorkerOutcome::BudgetSpent) => {}
            Some(WorkerOutcome::RestartLimit) => evidence
                .incomplete
                .push(Incomplete::RestartLimit { worker: plan.index }),
            Some(WorkerOutcome::Unfinished) | None => evidence
                .incomplete
                .push(Incomplete::Unfinished { worker: plan.index }),
        }
        let attempts = read_attempts(&worker)?;
        let stats = read_stats(
            &worker.join(STATS_DIR),
            plan.index,
            &manifest.harness.name,
            &mut evidence.incomplete,
        )?;
        let mut ends = BTreeMap::new();
        for attempt in &attempts {
            *ends.entry(attempt.end).or_default() += 1;
            let Some(pid) = attempt.pid else {
                continue;
            };
            if !stats.contains_key(&pid) {
                evidence.incomplete.push(Incomplete::MissingStats {
                    worker: plan.index,
                    attempt: attempt.attempt,
                    pid,
                });
            }
        }
        let mut counts = Counts::default();
        let processes = stats.len();
        for (pid, process) in &stats {
            if !attempts.iter().any(|attempt| attempt.pid == Some(*pid)) {
                evidence.incomplete.push(Incomplete::UnloggedProcess {
                    worker: plan.index,
                    pid: *pid,
                });
            }
            if process.complete {
                evidence.complete_totals.add(&process.counts);
            } else {
                evidence.incomplete.push(Incomplete::PartialStats {
                    worker: plan.index,
                    pid: *pid,
                });
            }
            counts.add(&process.counts);
        }
        evidence.totals.add(&counts);
        evidence.workers.push(WorkerEvidence {
            index: plan.index,
            outcome,
            attempts: ends,
            processes,
            counts,
        });
    }
    // The harness's properties count even if no file names them, so a run
    // without stats files leaves every property unapplied.
    let complete = &evidence.complete_totals.properties;
    let properties: BTreeSet<&String> = manifest
        .harness
        .properties
        .iter()
        .map(|property| &property.id)
        .chain(evidence.totals.properties.keys())
        .collect();
    evidence.unapplied = properties
        .into_iter()
        .filter(|id| !complete.get(*id).is_some_and(|counts| counts.applied()))
        .cloned()
        .collect();
    evidence.incomplete.sort();
    Ok(evidence)
}

/// Worker directory `dir`'s outcome record, if it wrote one.
fn outcome_record(dir: &Path) -> Result<Option<WorkerEnd>, CampaignError> {
    let path = dir.join(OUTCOME_FILE);
    if !path.exists() {
        return Ok(None);
    }
    read_record(&path, None).map(Some)
}

/// Every stats file of `harness` in `dir`, by process ID. A file that can't
/// be read, or that another harness wrote, joins `incomplete` instead.
fn read_stats(
    dir: &Path,
    worker: usize,
    harness: &str,
    incomplete: &mut Vec<Incomplete>,
) -> Result<BTreeMap<u32, ProcessStats>, CampaignError> {
    let mut stats = BTreeMap::new();
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(stats),
        Err(error) => {
            return Err(CampaignError::Io {
                context: format!("listing {}", dir.display()),
                error,
            })
        }
    };
    for entry in entries {
        let entry = entry.map_err(CampaignError::io(format!("listing {}", dir.display())))?;
        let file = entry.file_name().to_string_lossy().into_owned();
        let Some(pid) = ProcessStats::pid_of(&file) else {
            continue;
        };
        let read = ProcessStats::read(&entry.path()).and_then(|process| {
            if process.harness != harness {
                Err(format!(
                    "it holds harness {}'s counts, not {harness}'s",
                    process.harness
                ))
            } else if process.pid != pid {
                Err(format!("it holds process {}'s counts", process.pid))
            } else {
                Ok(process)
            }
        });
        match read {
            Ok(process) => {
                stats.insert(pid, process);
            }
            Err(error) => incomplete.push(Incomplete::UnreadableStats {
                worker,
                file,
                error,
            }),
        }
    }
    Ok(stats)
}
