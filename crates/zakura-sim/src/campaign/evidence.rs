//! A run's evidence: what its workers' fuzzer processes counted, summed per
//! worker and for the run, with every way the counts fall short
//! (explorer implementation plan, E4b-min).
//!
//! Each worker's processes write their stats files to the worker's
//! `stats/` directory ([`crate::explore::stats`]). The evidence matches
//! each file to the attempt in the worker's attempts log whose process has
//! its process ID and started while the attempt ran, so a reused process ID
//! or a stale copy matches no attempt. It lists each gap as an
//! [`Incomplete`]: a worker that didn't spend its budget, a process that
//! left no file, a file its process never marked complete, a complete file
//! whose process didn't exit cleanly, and a file no logged process wrote.
//! Only complete files that a logged process wrote, and whose attempt
//! exited cleanly, count toward [`Evidence::complete_totals`], and so
//! toward [`Evidence::unapplied`]. A process exits cleanly if its attempt
//! logged exit code 0 and no failure. The supervisor logs an attempt only
//! once its process has exited, so a running process, or one whose
//! supervisor stopped first, has no attempt, and its file is unlogged.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs, io,
    path::Path,
};

use serde::Serialize;

use super::{
    run::{
        read_attempts, read_end, worker_dir, Attempt, AttemptEnd, WorkerOutcome, OUTCOME_FILE,
        STATS_DIR,
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
    /// The counts of the complete stats files that logged processes wrote
    /// before they exited cleanly, summed.
    pub complete_totals: Counts,
    /// The properties, of the harness's and of any stats file, that no file
    /// in [`Evidence::complete_totals`] saw apply: none of their runs held
    /// with an applicable evaluation or violated them.
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
    /// A process the attempts log lists left no stats file, or the log,
    /// written before attempts recorded it, doesn't name its process.
    MissingStats {
        /// The worker.
        worker: usize,
        /// The attempt.
        attempt: u32,
        /// Its process ID, if the log names it.
        pid: Option<u32>,
    },
    /// A stats file its process never marked complete: the process stopped
    /// inside a run or without its exit handlers, or still runs. The file
    /// lacks the runs since its last checkpoint.
    PartialStats {
        /// The worker.
        worker: usize,
        /// Its process ID.
        pid: u32,
    },
    /// A logged process marked its stats file complete, but its attempt
    /// didn't exit cleanly: it exited with a code other than 0, a signal
    /// or the supervisor's deadline kill ended it, or the supervisor found
    /// a failure in its output. Its counts stay out of the complete
    /// totals.
    UncleanExit {
        /// The worker.
        worker: usize,
        /// The attempt.
        attempt: u32,
        /// Its process ID.
        pid: u32,
    },
    /// A stats file no process in the attempts log wrote: the supervisor
    /// stopped while the process ran, or the file is a stale copy or from
    /// an earlier process with the same ID. Its counts stay out of the
    /// complete totals.
    UnloggedProcess {
        /// The worker.
        worker: usize,
        /// Its process ID.
        pid: u32,
    },
    /// A stats file that can't be read, that another harness wrote, or
    /// whose name isn't `stats-<pid>.json`. Its counts are left out.
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
        // The latest attempt claims a file first, so a file whose process
        // ID an earlier attempt also had counts once.
        let mut logged = BTreeMap::new();
        for attempt in attempts.iter().rev() {
            *ends.entry(attempt.end).or_default() += 1;
            let wrote = attempt.pid.filter(|pid| {
                !logged.contains_key(pid)
                    && stats
                        .get(pid)
                        .is_some_and(|process| started_during(process, attempt))
            });
            match wrote {
                Some(pid) => {
                    logged.insert(pid, attempt);
                }
                None => evidence.incomplete.push(Incomplete::MissingStats {
                    worker: plan.index,
                    attempt: attempt.attempt,
                    pid: attempt.pid,
                }),
            }
        }
        let mut counts = Counts::default();
        let processes = stats.len();
        for (pid, process) in &stats {
            let attempt = logged.get(pid);
            if attempt.is_none() {
                evidence.incomplete.push(Incomplete::UnloggedProcess {
                    worker: plan.index,
                    pid: *pid,
                });
            }
            if !process.complete {
                evidence.incomplete.push(Incomplete::PartialStats {
                    worker: plan.index,
                    pid: *pid,
                });
            } else if let Some(attempt) = attempt {
                if exited_cleanly(attempt) {
                    evidence.complete_totals.add(&process.counts);
                } else {
                    evidence.incomplete.push(Incomplete::UncleanExit {
                        worker: plan.index,
                        attempt: attempt.attempt,
                        pid: *pid,
                    });
                }
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

/// True if `process` started while `attempt` ran. Both times are whole
/// seconds, and the process starts after the supervisor logs its attempt's
/// start.
fn started_during(process: &ProcessStats, attempt: &Attempt) -> bool {
    let ended = attempt
        .started_unix
        .saturating_add(attempt.millis.div_ceil(1000));
    (attempt.started_unix..=ended).contains(&process.started_unix)
}

/// True if `attempt`'s process exited with code 0, and the supervisor
/// found no failure in its output. A signal or the supervisor's deadline
/// kill leaves no code.
fn exited_cleanly(attempt: &Attempt) -> bool {
    attempt.code == Some(0) && attempt.failure.is_none()
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
/// be read, that another harness wrote, or that is named like a stats file
/// but not as a process writes one, such as `stats-07.json`, joins
/// `incomplete` instead.
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
        if !(file.starts_with("stats-") && file.ends_with(".json")) {
            continue;
        }
        let read = ProcessStats::pid_of(&file)
            .ok_or_else(|| "it isn't named stats-<pid>.json".to_owned())
            .and_then(|pid| Ok((pid, ProcessStats::read(&entry.path())?)))
            .and_then(|(pid, process)| {
                if process.harness != harness {
                    Err(format!(
                        "it holds harness {}'s counts, not {harness}'s",
                        process.harness
                    ))
                } else if process.pid != pid {
                    Err(format!("it holds process {}'s counts", process.pid))
                } else {
                    Ok((pid, process))
                }
            });
        match read {
            Ok((pid, process)) => {
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
