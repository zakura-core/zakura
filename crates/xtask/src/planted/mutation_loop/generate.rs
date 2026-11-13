//! The mutation loop's generator lane (mutation plan, mut-4.3 and mut-4.4).
//!
//! With `--generate-runs <n>`, each case also gets one generator lane per
//! seed of `--generate-seeds`. A lane searches from scratch, on one
//! generator harness, for an input that violates the case's property. The
//! default mode, `inproc`, runs `zakura-sim generate`, which draws `n`
//! inputs from the seed and runs them in its own process. The opt-in mode,
//! `libfuzzer`, runs one coverage-guided campaign per seed with the pilot's
//! campaign lane ([`super::super::lane`]), from an empty corpus, for
//! `--generate-seconds`. `zakura-sim campaign start` reserves libFuzzer's
//! `-runs`, so a campaign is bounded by time, not by runs.
//!
//! - **Harness.** With `--generate-harness auto`, a property that the work
//!   list records under `[[no-generator]]` gets no lane, and its case says
//!   why. Any other case runs on its witness's harness if that harness
//!   reports the property, else on the first harness that does, as
//!   `zakura-sim generators` lists them. `--generate-harness <name>` runs
//!   every case on that harness.
//! - **Budget.** An `inproc` search draws `--generate-runs` inputs, or the
//!   count `--generate-harness-runs <harness>=<n>` sets for its harness. A
//!   harness whose runs take seconds gets a smaller count, and the report
//!   records it.
//! - **Baselines.** One search per harness and seed runs on the unpatched
//!   build first. A search that a run failure ended, such as an OOM, a
//!   timeout or a signal, reruns alone with the host's memory. A search
//!   that stopped at an `Active` violation that a fresh process confirms is
//!   a property finding: ordinary property testing that found a bug on the
//!   pinned revision, or a property defect. The report lists it under
//!   `property_findings`. Any other search that isn't clean is a generator
//!   baseline failure, under `generator_baseline_failures`. Either way that
//!   harness and seed's lane is `baseline-fails` for every case. The
//!   witness lane doesn't depend on it.
//! - **Exclusions.** Every `inproc` search skips each committed input under
//!   [`EXCLUDED_DIRS`], by SHA-256, so it can't rediscover a witness. A
//!   `libfuzzer` campaign starts from an empty corpus and excludes nothing.
//! - **Classes.** A lane gets the witness lane's classes
//!   ([`classify::generated`]).
//! - **Confirmation.** A kill's input must violate the same ID in a fresh
//!   `zakura-sim confirm`, and its trace must replay Exact. Otherwise the
//!   lane is a harness failure (`unconfirmed`). The loop then shrinks the
//!   confirmed trace with `zakura-sim shrink` under `ZAKURA_SIM_EXPECT`,
//!   within [`SHRINK_CANDIDATES`] and [`shrink_wall`].
//! - **Unpatched check (mut-5).** The unpatched build's `zakura-sim` reruns
//!   each confirmed kill's input with `record`, since a mutant's trace holds
//!   picks the unpatched build leaves. It replays the shrunk trace, which
//!   has no input, with `--allow-build-mismatch`, if the shrink finished.
//!   The loop reads each run's report ([`judge_unpatched`]). If the
//!   unpatched build violates the killing ID on the confirmed trace too, the
//!   kill shows a defect of the pinned revision, not of the mutant: the lane
//!   is `baseline-fails` ([`unpatched_class`]). A violation on either trace
//!   is a property finding ([`GeneratorReport::finding`]). The shrink kept
//!   only the ID, not the mutant's effect, so a violation on the shrunk
//!   trace alone leaves the kill. A run that passes its deadline, that the
//!   memory cap kills, or that writes no report judging the ID is
//!   `undecided`. A kill whose confirmed trace is undecided there is a
//!   harness failure (`unconfirmed`): only a hold on the unpatched build
//!   shows the mutant caused the violation.
//!
//! A mutant's search runs once per harness and seed, and the loop
//! classifies each of the mutant's cases from it. A search, or each
//! campaign child, runs under the witness lane's memory cap. Its deadline
//! is its baseline's time times the witness lane's factor, and at least the
//! witness lane's minimum. A campaign's `start` also gets at least its
//! budget and the pilot's grace.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::Mutex,
    thread,
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{
    super::lane::{
        self, campaign_failure, confirm_and_replay, left_failures, run_lane_child, BuildFailure,
        CampaignBounds, CampaignFacts, Confirm, LaneError, LaneSpec, EXPECT_VAR,
    },
    classify::{self, kill_violations, Class, Classified, GenerateFacts, GenerateStop, Reason},
    host_memory, key, scoped,
    triage::Triage,
    Built, LoopCase, Ran, Runnable, Runner, Slot, BASELINE_DEADLINE, DEADLINE_FACTOR, MIN_DEADLINE,
    WORK_LIST,
};
use crate::{
    planted::pilot::hex,
    sim_minimize::{self, ChildEnd},
    BoxError,
};

/// The seeds a lane searches by default: the pilot's discovery seeds.
const DEFAULT_SEEDS: &[u64] = &[11, 23, 47, 89, 179];

/// How long a `libfuzzer` lane's campaign runs by default: the pilot's
/// `seconds_per_seed`.
const DEFAULT_SECONDS: u64 = 600;

/// The directories whose committed inputs every search excludes, relative
/// to the repository root.
pub(super) const EXCLUDED_DIRS: &[&str] = &[
    "qa/planted",
    "qa/native-mutations",
    "crates/zakura-sim/tests/fixtures",
];

/// The file every `inproc` search reads its exclusions from, under
/// `--out`.
const EXCLUDE_FILE: &str = "generate/exclude.txt";

/// The empty corpus every `libfuzzer` campaign starts from, under `--out`.
const EMPTY_CORPUS: &str = "generate/empty-corpus";

/// The directory, under a search's, where `zakura-sim generate` writes.
pub(super) const SEARCH_DIR: &str = "search";

/// `zakura-sim generate`'s report file, under [`SEARCH_DIR`]. It is
/// `zakura_sim::explore::generate::REPORT_FILE`.
pub(super) const REPORT_FILE: &str = "generate.json";

/// Where the loop keeps the unpatched build's `zakura-sim`, under `--out`.
/// Slot 0 rebuilds mutants in its target directory, so the lanes can't
/// read that copy once the baselines ran.
const UNPATCHED_SIM: &str = "baseline/zakura-sim";

/// The most candidates a kill's shrink tries.
const SHRINK_CANDIDATES: u32 = 500;

/// The shortest wall a kill's shrink gets.
const SHRINK_WALL: Duration = Duration::from_secs(120);

/// The candidate runs a kill's shrink can afford at least, at the
/// search's time per run.
const SHRINK_RUNS: u32 = 10;

/// How long a kill's shrink runs when a search's run takes `per_run`:
/// [`SHRINK_RUNS`] runs, and at least [`SHRINK_WALL`]. A harness whose run
/// takes minutes would otherwise never finish one candidate.
pub(super) fn shrink_wall(per_run: Duration) -> Duration {
    (per_run * SHRINK_RUNS).max(SHRINK_WALL)
}

/// A search's time per run: `millis` over `runs`, or zero without a count.
fn per_run(millis: u64, runs: Option<u64>) -> Duration {
    runs.filter(|runs| *runs > 0)
        .map_or(Duration::ZERO, |runs| Duration::from_millis(millis / runs))
}

/// The shortest deadline a kill's confirm or replay child gets: the pilot
/// catalogue's `child_seconds`.
const CONFIRM_CHILD: Duration = Duration::from_secs(300);

/// The runs a kill's confirm or replay child can afford at least, at the
/// search's time per run. A confirmation runs the input about four times.
const CONFIRM_RUNS: u32 = 10;

/// The deadline of one confirm or replay child of a kill when a search's
/// run takes `per_run`: [`CONFIRM_RUNS`] runs, and at least
/// [`CONFIRM_CHILD`]. A harness whose run takes minutes would otherwise
/// never confirm.
pub(super) fn confirm_child(per_run: Duration) -> Duration {
    (per_run * CONFIRM_RUNS).max(CONFIRM_CHILD)
}

/// How a lane searches.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(in crate::planted) enum GenerateMode {
    /// `zakura-sim generate`: deterministic, bounded by runs.
    #[default]
    Inproc,
    /// One libFuzzer campaign: coverage-guided, bounded by time.
    Libfuzzer,
}

/// The `--generate-*` flags.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::planted) struct GenerateFlags {
    /// `--generate-runs`: 0 turns the lane off.
    pub(in crate::planted) runs: u64,
    /// `--generate-seeds`.
    pub(in crate::planted) seeds: Vec<u64>,
    /// `--generate-harness`: `None` for `auto`.
    pub(in crate::planted) harness: Option<String>,
    /// `--generate-mode`.
    pub(in crate::planted) mode: GenerateMode,
    /// `--generate-seconds`: a `libfuzzer` campaign's budget.
    pub(in crate::planted) seconds: u64,
    /// `--generate-harness-runs`: the runs an `inproc` search on a harness
    /// draws instead of `runs`.
    pub(in crate::planted) harness_runs: BTreeMap<String, u64>,
}

impl Default for GenerateFlags {
    fn default() -> Self {
        GenerateFlags {
            runs: 0,
            seeds: DEFAULT_SEEDS.to_vec(),
            harness: None,
            mode: GenerateMode::Inproc,
            seconds: DEFAULT_SECONDS,
            harness_runs: BTreeMap::new(),
        }
    }
}

impl GenerateFlags {
    /// Reads `flag`'s `value` if `flag` is a `--generate-*` flag. Returns
    /// false if it isn't one.
    pub(in crate::planted) fn parse(&mut self, flag: &str, value: &str) -> Result<bool, String> {
        let number = |value: &str| {
            value
                .parse::<u64>()
                .map_err(|_| format!("{flag} takes a whole number, not `{value}`"))
        };
        match flag {
            "--generate-runs" => self.runs = number(value)?,
            "--generate-seeds" => {
                self.seeds = value
                    .split(',')
                    .map(|seed| number(seed.trim()))
                    .collect::<Result<_, _>>()?;
            }
            "--generate-harness" => self.harness = (value != "auto").then(|| value.to_owned()),
            "--generate-harness-runs" => {
                let (harness, runs) = value
                    .split_once('=')
                    .ok_or_else(|| format!("{flag} takes `<harness>=<runs>`, not `{value}`"))?;
                let runs = number(runs)?;
                if runs == 0 {
                    return Err(format!("{flag} takes a positive count"));
                }
                if self.harness_runs.insert(harness.to_owned(), runs).is_some() {
                    return Err(format!("{flag} gives {harness} twice"));
                }
            }
            "--generate-mode" => {
                self.mode = match value {
                    "inproc" => GenerateMode::Inproc,
                    "libfuzzer" => GenerateMode::Libfuzzer,
                    _ => {
                        return Err(format!(
                            "--generate-mode takes `inproc` or `libfuzzer`, not `{value}`"
                        ))
                    }
                };
            }
            "--generate-seconds" => {
                self.seconds = number(value)?;
                if self.seconds == 0 {
                    return Err("--generate-seconds takes a positive count".to_owned());
                }
            }
            _ => return Ok(false),
        }
        Ok(true)
    }

    /// True if the loop runs generator lanes.
    pub(in crate::planted) fn on(&self) -> bool {
        self.runs > 0
    }

    /// The runs an `inproc` search on `harness` draws.
    pub(in crate::planted) fn runs_for(&self, harness: &str) -> u64 {
        self.harness_runs.get(harness).copied().unwrap_or(self.runs)
    }
}

/// The generator lanes' setup, as the report records it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(in crate::planted) struct GenerateConfig {
    pub(in crate::planted) mode: GenerateMode,
    /// The runs an `inproc` search draws.
    pub(in crate::planted) runs: u64,
    /// The runs an `inproc` search on each of these harnesses draws
    /// instead.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub(in crate::planted) harness_runs: BTreeMap<String, u64>,
    pub(in crate::planted) seeds: Vec<u64>,
    /// `auto`, or the one harness every lane searches.
    pub(in crate::planted) harness: String,
    /// A `libfuzzer` campaign's budget.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(in crate::planted) seconds: Option<u64>,
    /// How many committed inputs every search excludes.
    pub(in crate::planted) excluded_inputs: usize,
}

/// A fresh-process step after a kill: its confirmation or its shrink.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(in crate::planted) struct FollowUp {
    /// True if the step succeeded.
    pub(in crate::planted) ok: bool,
    /// The confirmed or shrunk trace, relative to `--out`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(in crate::planted) trace: Option<PathBuf>,
    /// Why the step failed, or why a confirmed kill's trace is no witness.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(in crate::planted) detail: Option<String>,
}

impl FollowUp {
    pub(in crate::planted) fn failed(detail: String) -> FollowUp {
        FollowUp {
            ok: false,
            trace: None,
            detail: Some(detail),
        }
    }
}

/// One seed's generator lane for one case.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(in crate::planted) struct GeneratorReport {
    pub(in crate::planted) harness: String,
    pub(in crate::planted) seed: u64,
    pub(in crate::planted) mode: GenerateMode,
    pub(in crate::planted) result: Classified,
    /// The ID whose violation stopped the search.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(in crate::planted) violated: Option<String>,
    /// The runs to the kill, or every run of a search that didn't stop.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(in crate::planted) runs: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(in crate::planted) millis: Option<u64>,
    /// The input that stopped the search, relative to `--out`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(in crate::planted) input: Option<PathBuf>,
    /// The scenario family whose tag starts that input, or `untagged`, as
    /// `zakura-sim generate` reports it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(in crate::planted) family: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(in crate::planted) confirmation: Option<FollowUp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(in crate::planted) shrunk: Option<FollowUp>,
    /// The kill's runs on the unpatched build: the confirmed trace's input
    /// rerun, then the shrunk trace's replay if the shrink finished.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(in crate::planted) unpatched: Vec<UnpatchedCheck>,
    /// The lane's recorded triage ([`super::triage`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(in crate::planted) triage: Option<Triage>,
    /// The search's evidence, relative to `--out`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(in crate::planted) log: Option<PathBuf>,
}

/// What the unpatched build does on a kill's trace.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(in crate::planted) enum OnUnpatched {
    /// The run ended, and the killing ID isn't violated.
    Holds,
    /// The run violates the killing ID: a property finding.
    Violated,
    /// The run wrote no report that judges the killing ID, such as when it
    /// passed its deadline or a replay left the trace's picks.
    Undecided,
}

/// Which of a kill's traces the unpatched build ran: the confirmed trace's
/// input reruns from scratch, and the shrunk trace replays.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(in crate::planted) enum KillTrace {
    /// The trace `zakura-sim confirm` recorded from the search's input.
    Confirmed,
    /// The confirmed trace after `zakura-sim shrink`.
    Shrunk,
}

impl KillTrace {
    fn name(self) -> &'static str {
        match self {
            KillTrace::Confirmed => "confirmed",
            KillTrace::Shrunk => "shrunk",
        }
    }
}

/// A kill's trace run on the unpatched build: its input rerun with
/// `record`, or the trace replayed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(in crate::planted) struct UnpatchedCheck {
    pub(in crate::planted) trace: KillTrace,
    pub(in crate::planted) outcome: OnUnpatched,
    /// What showed it, in words.
    pub(in crate::planted) detail: String,
    /// The run's log, relative to `--out`.
    pub(in crate::planted) log: PathBuf,
}

impl GeneratorReport {
    /// A lane that didn't search, with `result`.
    pub(in crate::planted) fn unsearched(
        harness: &str,
        seed: u64,
        mode: GenerateMode,
        result: Classified,
    ) -> GeneratorReport {
        GeneratorReport {
            harness: harness.to_owned(),
            seed,
            mode,
            result,
            violated: None,
            runs: None,
            millis: None,
            input: None,
            family: None,
            confirmation: None,
            shrunk: None,
            unpatched: Vec::new(),
            triage: None,
            log: None,
        }
    }

    /// The property finding this lane shows, if the unpatched build
    /// violates the killing ID on one of the kill's traces too: the shrunk
    /// trace's replay if it violates, as the smaller reproduction, else the
    /// confirmed trace's.
    pub(in crate::planted) fn finding(&self) -> Option<PropertyFinding> {
        let violated = |trace| {
            self.unpatched
                .iter()
                .find(|check| check.trace == trace && check.outcome == OnUnpatched::Violated)
        };
        let check = violated(KillTrace::Shrunk).or_else(|| violated(KillTrace::Confirmed))?;
        Some(PropertyFinding {
            harness: self.harness.clone(),
            seed: self.seed,
            mode: self.mode,
            property: self.violated.clone(),
            detail: check.detail.clone(),
            runs: self.runs,
            millis: self.millis.unwrap_or_default(),
            input: self.input.clone(),
            family: self.family.clone(),
            confirmation: self.confirmation.clone(),
            shrunk: self.shrunk.clone(),
            log: check.log.clone(),
        })
    }
}

/// How the unpatched build judged `id` on a kill's `trace`, from the
/// report that `zakura-sim <command>` wrote, `report`, or `None` if it wrote
/// none. `command` is `record` for a rerun of the trace's input and
/// `replay` for a replay of the trace. `ended` says how the run ended, for
/// an undecided judgment.
pub(in crate::planted) fn judge_unpatched(
    id: &str,
    trace: KillTrace,
    command: &str,
    report: Option<&str>,
    ended: &str,
) -> (OnUnpatched, String) {
    let trace = trace.name();
    let undecided = |why: String| (OnUnpatched::Undecided, why);
    let Some(report) = report else {
        return undecided(format!(
            "the unpatched `zakura-sim {command}` of the {trace} trace wrote no report; it {ended}"
        ));
    };
    let Ok(report) = serde_json::from_str::<serde_json::Value>(report) else {
        return undecided(format!(
            "the unpatched `zakura-sim {command}` of the {trace} trace wrote a report that isn't JSON"
        ));
    };
    let entry = report["properties"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|entry| entry["property"] == id);
    let Some(entry) = entry else {
        return undecided(format!(
            "the unpatched `zakura-sim {command}` of the {trace} trace doesn't report {id}"
        ));
    };
    let violations = entry["violations"].as_array().map_or(0, Vec::len);
    if violations > 0 {
        (
            OnUnpatched::Violated,
            format!("the {trace} trace violates {id} on the unpatched build too"),
        )
    } else {
        (
            OnUnpatched::Holds,
            format!("{id} holds in the unpatched `zakura-sim {command}` of the {trace} trace"),
        )
    }
}

/// A kill's class once the unpatched build ran its traces. A kill keeps
/// `result` only if the killing ID holds when the unpatched build reruns the
/// confirmed trace's input. If the unpatched build violates the ID there
/// too, the lane is `baseline-fails`: the defect is the pinned revision's.
/// If that run is undecided, or never ran, the lane is a harness failure
/// (`unconfirmed`): the unpatched build couldn't judge the kill's input, so
/// nothing shows the mutant caused the violation. The shrunk trace's run
/// doesn't count: the shrink kept the ID's violation, not the mutant's part
/// in it. A `result` that isn't a kill, such as an unconfirmed one, stays.
pub(super) fn unpatched_class(result: Classified, checks: &[UnpatchedCheck]) -> Classified {
    if !matches!(
        result.class,
        Class::KilledByNamedRule | Class::KilledByOtherRule
    ) {
        return result;
    }
    let unconfirmed = |why: &str| {
        Classified::new(
            Class::HarnessFailure,
            Some(Reason::Unconfirmed),
            format!(
                "{}, but the unpatched build couldn't judge the kill's input: {why}",
                result.detail
            ),
        )
    };
    let confirmed = checks
        .iter()
        .find(|check| check.trace == KillTrace::Confirmed);
    match confirmed {
        Some(check) => match check.outcome {
            OnUnpatched::Holds => result,
            OnUnpatched::Violated => Classified::new(
                Class::BaselineFails,
                None,
                format!("{}, but {}", result.detail, check.detail),
            ),
            OnUnpatched::Undecided => unconfirmed(&check.detail),
        },
        None => unconfirmed("the confirmed trace never ran there"),
    }
}

/// What a search on the unpatched build stopped at.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(in crate::planted) struct PropertyFinding {
    pub(in crate::planted) harness: String,
    pub(in crate::planted) seed: u64,
    pub(in crate::planted) mode: GenerateMode,
    /// The `Active` ID violated, for a violation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(in crate::planted) property: Option<String>,
    /// What stopped the search, in words.
    pub(in crate::planted) detail: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(in crate::planted) runs: Option<u64>,
    pub(in crate::planted) millis: u64,
    /// The input that stopped the search, relative to `--out`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(in crate::planted) input: Option<PathBuf>,
    /// The scenario family whose tag starts that input, or `untagged`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(in crate::planted) family: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(in crate::planted) confirmation: Option<FollowUp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(in crate::planted) shrunk: Option<FollowUp>,
    /// The search's evidence, relative to `--out`.
    pub(in crate::planted) log: PathBuf,
}

/// A harness and a seed: one search per build.
pub(super) type Pair = (String, u64);

/// A harness and seed's unpatched search that wasn't clean, other than a
/// confirmed property finding: a run failure, a harness failure on the
/// pinned revision, or a violation that didn't confirm.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(in crate::planted) struct GeneratorBaselineFailure {
    pub(in crate::planted) harness: String,
    pub(in crate::planted) seed: u64,
    pub(in crate::planted) mode: GenerateMode,
    /// A harness failure, with its reason.
    pub(in crate::planted) result: Classified,
    /// True if a run failure ended the first search, and this result is
    /// the rerun alone with the host's memory.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub(in crate::planted) rerun_alone: bool,
    /// The search's evidence, relative to `--out`.
    pub(in crate::planted) log: PathBuf,
}

/// A harness and seed's unpatched search.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct GenBaseline {
    /// The class every lane of the pair gets instead of searching, a
    /// `baseline-fails` one, or `None` for a clean search.
    pub(super) failure: Option<Classified>,
    pub(super) millis: u64,
}

/// What a search on the unpatched build showed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Unpatched {
    /// It ran every input with no `Active` violation or failure.
    Clean,
    /// It stopped at a violation of this `Active` ID.
    Violation(String),
    /// It gave no result, or stopped at a harness failure, an abandonment
    /// limit or a panic. `rerun` is true for a run failure, such as an
    /// OOM, a timeout or a signal, that a run alone may not repeat.
    Failed { result: Classified, rerun: bool },
}

/// What every generator lane shares once the baselines ran.
pub(super) struct Lanes {
    pub(super) flags: GenerateFlags,
    /// Each case's harness, or why it gets no lane, by case index.
    pub(super) plans: Vec<Result<String, String>>,
    /// Each harness and seed's unpatched search.
    pub(super) baselines: BTreeMap<Pair, GenBaseline>,
    /// The digests every `inproc` search excludes.
    pub(super) exclude: PathBuf,
    /// The empty corpus every `libfuzzer` campaign starts from.
    pub(super) corpus: PathBuf,
    /// Each harness's fuzz target, for `libfuzzer` lanes.
    pub(super) targets: BTreeMap<String, String>,
    /// The unpatched build's `zakura-sim`, copied out of slot 0's target.
    pub(super) unpatched_sim: PathBuf,
}

impl Lanes {
    /// One report per seed, each with `result`, for case `index`'s lanes,
    /// which didn't search. Empty if the case has no lane.
    pub(super) fn unsearched(&self, index: usize, result: &Classified) -> Vec<GeneratorReport> {
        let Ok(harness) = &self.plans[index] else {
            return Vec::new();
        };
        self.flags
            .seeds
            .iter()
            .map(|seed| {
                GeneratorReport::unsearched(harness, *seed, self.flags.mode, result.clone())
            })
            .collect()
    }
}

/// One search's evidence.
pub(super) enum Searched {
    /// A `zakura-sim generate` process: how it ended, its records and its
    /// report.
    Inproc(Ran),
    /// A campaign's records, or the class of the lane failure that left
    /// none.
    Campaign(Result<lane::Campaign, Classified>),
}

/// One harness and seed's search on one build.
pub(super) struct Search {
    pub(super) searched: Searched,
    pub(super) millis: u64,
    /// Where its evidence went.
    pub(super) dir: PathBuf,
}

/// A kill: the ID whose violation stopped a search, and the saved input
/// that violates it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Kill {
    pub(super) id: String,
    pub(super) input: Option<PathBuf>,
}

impl Search {
    /// The search's class for `property`, allowing `also_violates`, and the
    /// kill that decided a killed class.
    pub(super) fn classify(
        &self,
        property: &str,
        also_violates: &[String],
    ) -> (Classified, Option<Kill>) {
        let result = match &self.searched {
            Searched::Inproc(ran) => classify::generated(
                property,
                also_violates,
                &ran.observed,
                ran.generated.as_ref(),
            ),
            Searched::Campaign(Ok(campaign)) => {
                campaign_class(property, also_violates, &campaign.facts(property))
            }
            Searched::Campaign(Err(failure)) => failure.clone(),
        };
        if !matches!(
            result.class,
            Class::KilledByNamedRule | Class::KilledByOtherRule
        ) {
            return (result, None);
        }
        let id = match &self.searched {
            Searched::Inproc(Ran {
                generated:
                    Some(GenerateFacts {
                        stop: GenerateStop::Violation { property: id },
                        ..
                    }),
                ..
            }) => id.clone(),
            _ => result.other.clone().unwrap_or_else(|| property.to_owned()),
        };
        let input = self.input(&id);
        (result, Some(Kill { id, input }))
    }

    /// The saved input that violates `id`, absolute.
    fn input(&self, id: &str) -> Option<PathBuf> {
        match &self.searched {
            Searched::Inproc(ran) => ran.generated.as_ref()?.input.clone(),
            Searched::Campaign(Ok(campaign)) => campaign
                .facts(id)
                .intended_input
                .map(|input| self.dir.join("store").join(input)),
            Searched::Campaign(Err(_)) => None,
        }
    }

    /// The scenario family of the input that stopped an `inproc` search.
    fn family(&self) -> Option<String> {
        match &self.searched {
            Searched::Inproc(ran) => ran.generated.as_ref()?.family.clone(),
            Searched::Campaign(_) => None,
        }
    }

    /// The runs the search took: to its kill, or all of them.
    fn runs(&self) -> Option<u64> {
        match &self.searched {
            Searched::Inproc(ran) => ran.generated.as_ref().map(GenerateFacts::runs),
            Searched::Campaign(Ok(campaign)) => Some(campaign.facts("").runs),
            Searched::Campaign(Err(_)) => None,
        }
    }

    /// What the search showed on the unpatched build.
    pub(super) fn unpatched(&self) -> Unpatched {
        let failed = |reason, detail: String, rerun| Unpatched::Failed {
            result: Classified::new(Class::HarnessFailure, Some(reason), detail),
            rerun,
        };
        match &self.searched {
            Searched::Inproc(ran) => {
                let facts = ran.generated.as_ref();
                if let Some(result) = classify::generator_failure(&ran.observed, facts) {
                    return Unpatched::Failed {
                        result,
                        rerun: true,
                    };
                }
                let facts = facts.expect("a search with no run failure wrote its report");
                let detail = classify::stop_detail(&facts.stop, facts.runs()).unwrap_or_default();
                match &facts.stop {
                    GenerateStop::Clean => Unpatched::Clean,
                    GenerateStop::Violation { property } => Unpatched::Violation(property.clone()),
                    GenerateStop::HarnessFailure { .. } => {
                        failed(Reason::HarnessEnd, detail, false)
                    }
                    GenerateStop::Abandoned { .. } => failed(Reason::Abandoned, detail, false),
                    GenerateStop::Panic { .. } => failed(Reason::Panic, detail, false),
                }
            }
            Searched::Campaign(Err(result)) => Unpatched::Failed {
                rerun: result.reason == Some(Reason::Timeout),
                result: result.clone(),
            },
            Searched::Campaign(Ok(campaign)) => {
                let facts = campaign.facts("");
                if let Some(failure) = campaign_run_failure(&facts) {
                    return Unpatched::Failed {
                        rerun: !facts.killed.is_empty(),
                        result: failure,
                    };
                }
                if let Some(id) = facts.active.first() {
                    return Unpatched::Violation(id.clone());
                }
                match campaign_failure(&facts) {
                    Some(why) => failed(
                        Reason::CampaignIncomplete,
                        format!("the campaign {why}"),
                        false,
                    ),
                    None => Unpatched::Clean,
                }
            }
        }
    }
}

/// The run failure that keeps a campaign from giving any result, even
/// beside a violation: it started from inputs, or a worker timed out or
/// died, or it saved a failure that isn't a violation. As the pilot's
/// discovery lane reads it.
fn campaign_run_failure(facts: &CampaignFacts) -> Option<Classified> {
    let harness =
        |reason, detail| Some(Classified::new(Class::HarnessFailure, Some(reason), detail));
    if facts.initial_inputs > 0 {
        return harness(
            Reason::CampaignIncomplete,
            format!(
                "the campaign started from {} inputs, not an empty corpus",
                facts.initial_inputs
            ),
        );
    }
    let why = left_failures(facts)?;
    if facts.killed.is_empty() {
        harness(Reason::HarnessEnd, format!("the campaign {why}"))
    } else {
        harness(Reason::Timeout, format!("the campaign {why}"))
    }
}

/// A `libfuzzer` lane's class for `property`, allowing `also_violates`, by
/// the witness lane's rules and the pilot's completeness checks. A run
/// failure ([`campaign_run_failure`]) is a harness failure. Only the
/// `active` bucket's violations count; a `known` or `draft` one is a note,
/// as in an `inproc` search. A violation of an ID other than `property` and
/// `also_violates` kills by another rule. A violation of `property` kills
/// by the named rule. A violation of `also_violates` alone kills by another
/// rule. Without a violation, a campaign that didn't complete, or whose
/// evidence falls short, is a harness failure (`incomplete`). A complete one
/// survived: `held` if `property` applied in a complete process, else
/// `not-applicable`.
pub(super) fn campaign_class(
    property: &str,
    also_violates: &[String],
    facts: &CampaignFacts,
) -> Classified {
    let other = |id: &String| Classified {
        other: Some(id.clone()),
        ..Classified::new(
            Class::KilledByOtherRule,
            None,
            format!("the campaign violates {id} after {} runs", facts.runs),
        )
    };
    if let Some(failure) = campaign_run_failure(facts) {
        return failure;
    }
    if let Some(id) = facts
        .active
        .iter()
        .find(|id| *id != property && !also_violates.contains(id))
    {
        return other(id);
    }
    if facts.active.iter().any(|id| id == property) {
        return Classified::new(
            Class::KilledByNamedRule,
            None,
            format!("the campaign violates {property} after {} runs", facts.runs),
        );
    }
    if let Some(id) = facts.active.first() {
        return other(id);
    }
    if let Some(why) = campaign_failure(facts) {
        return Classified::new(
            Class::HarnessFailure,
            Some(Reason::CampaignIncomplete),
            format!("the campaign {why}"),
        );
    }
    if facts.applied {
        Classified::new(
            Class::Survived,
            Some(Reason::Held),
            format!(
                "{property} held: it applied in {} of {} runs",
                facts.applicable_runs, facts.runs
            ),
        )
    } else if facts.incomplete_runs > 0 {
        Classified::new(
            Class::Survived,
            Some(Reason::Incomplete),
            format!(
                "{property} ended with incomplete observations in {} of {} runs and never applied",
                facts.incomplete_runs, facts.runs
            ),
        )
    } else {
        Classified::new(
            Class::Survived,
            Some(Reason::NotApplicable),
            format!("{property} never applied in {} runs", facts.runs),
        )
    }
}

/// A lane failure's class: a fuzz target that doesn't compile is
/// `did-not-compile`, and any other failure a harness failure.
fn lane_failure(error: LaneError) -> Classified {
    match error {
        LaneError::Build(BuildFailure::Rejected(why)) => {
            Classified::new(Class::DidNotCompile, None, why)
        }
        LaneError::Build(BuildFailure::Broken(why)) => {
            Classified::new(Class::HarnessFailure, Some(Reason::Build), why)
        }
        LaneError::Killed(why) => {
            Classified::new(Class::HarnessFailure, Some(Reason::Timeout), why)
        }
        LaneError::Run(why) => {
            Classified::new(Class::HarnessFailure, Some(Reason::NoEndRecord), why)
        }
    }
}

/// Each case's generator harness, or why it gets none, by case index.
///
/// `forced` is `--generate-harness <name>`, which every case runs on.
/// Otherwise a property in `no_generator` gets that record's reason, and
/// any other case its witness's harness if `map` lists that harness for the
/// property, else the first harness `map` lists. With `targets`, for
/// `libfuzzer` lanes, a harness also needs a fuzz target.
pub(super) fn plan(
    cases: &[LoopCase],
    runnables: &[Runnable],
    map: &BTreeMap<String, Vec<String>>,
    no_generator: &BTreeMap<String, String>,
    forced: Option<&str>,
    targets: Option<&BTreeMap<String, String>>,
) -> Vec<Result<String, String>> {
    cases
        .iter()
        .zip(runnables)
        .map(|(case, runnable)| {
            let harness = match forced {
                Some(harness) => harness.to_owned(),
                None => {
                    if let Some(reason) = no_generator.get(&case.property) {
                        return Err(format!("no-generator: {reason}"));
                    }
                    let candidates = map.get(&case.property).map_or(&[][..], Vec::as_slice);
                    let own = match runnable {
                        Runnable::Record { harness, .. } => {
                            candidates.iter().find(|candidate| *candidate == harness)
                        }
                        _ => None,
                    };
                    own.or(candidates.first())
                        .cloned()
                        .ok_or_else(|| format!("no generator harness reports {}", case.property))?
                }
            };
            match targets {
                Some(targets) if !targets.contains_key(&harness) => {
                    Err(format!("no fuzz target runs {harness}"))
                }
                _ => Ok(harness),
            }
        })
        .collect()
}

/// The work list's `[[no-generator]]` records in `tree`: each property's
/// reason.
pub(super) fn no_generator(tree: &Path) -> Result<BTreeMap<String, String>, BoxError> {
    #[derive(Deserialize)]
    struct List {
        #[serde(default, rename = "no-generator")]
        no_generator: Vec<NoGenerator>,
    }
    #[derive(Deserialize)]
    struct NoGenerator {
        reason: String,
        properties: Vec<String>,
    }
    let list: List = toml::from_str(&fs::read_to_string(tree.join(WORK_LIST))?)?;
    Ok(list
        .no_generator
        .into_iter()
        .flat_map(|record| {
            let reason = record.reason;
            record
                .properties
                .into_iter()
                .map(move |id| (id, reason.clone()))
        })
        .collect())
}

/// `zakura-sim generators`: each property's generator harnesses.
fn generator_map(sim: &Path) -> Result<BTreeMap<String, Vec<String>>, BoxError> {
    let output = Command::new(sim).arg("generators").output()?;
    if !output.status.success() {
        return Err(format!(
            "`zakura-sim generators` failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    Ok(serde_json::from_slice(&output.stdout)?)
}

/// Each harness's first fuzz target in `tree`'s fuzz package.
fn harness_targets(tree: &Path) -> Result<BTreeMap<String, String>, BoxError> {
    let mut targets = BTreeMap::new();
    for (target, harness) in sim_minimize::fuzz_targets(tree)? {
        if let Some(harness) = harness {
            targets.entry(harness).or_insert(target);
        }
    }
    Ok(targets)
}

/// Writes the SHA-256 of every committed input under [`EXCLUDED_DIRS`] in
/// `tree` to `file`, one per line. Returns how many it wrote.
pub(super) fn write_exclusions(tree: &Path, file: &Path) -> Result<usize, BoxError> {
    let mut digests = BTreeSet::new();
    let mut pending: Vec<PathBuf> = EXCLUDED_DIRS.iter().map(|dir| tree.join(dir)).collect();
    while let Some(dir) = pending.pop() {
        let Ok(entries) = fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries {
            let path = entry?.path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|extension| extension == "bin") {
                digests.insert(hex(&Sha256::digest(fs::read(&path)?)));
            }
        }
    }
    if let Some(parent) = file.parent() {
        fs::create_dir_all(parent)?;
    }
    let text: String = digests.iter().map(|digest| format!("{digest}\n")).collect();
    fs::write(file, text)?;
    Ok(digests.len())
}

/// The other IDs that the input whose violation of `id` stopped `searched`
/// may also violate, which its fresh confirmation allows: those the kill's
/// end record shows ([`kill_violations`]), or, for a campaign, every other
/// `active` ID it saved.
pub(super) fn co_violations(searched: &Searched, id: &str) -> Vec<String> {
    let violated: Vec<String> = match searched {
        Searched::Inproc(ran) => kill_violations(id, &ran.observed.records)
            .map(<[String]>::to_vec)
            .unwrap_or_default(),
        Searched::Campaign(Ok(campaign)) => campaign.facts(id).active,
        Searched::Campaign(Err(_)) => Vec::new(),
    };
    violated.into_iter().filter(|other| other != id).collect()
}

/// A kill's class once its fresh `confirmation` ran: `result` if it
/// confirmed, else a harness failure, `unconfirmed`.
pub(super) fn confirmed(result: Classified, confirmation: &FollowUp) -> Classified {
    if confirmation.ok {
        return result;
    }
    Classified::new(
        Class::HarnessFailure,
        Some(Reason::Unconfirmed),
        format!(
            "{}, and the kill doesn't confirm: {}",
            result.detail,
            confirmation.detail.as_deref().unwrap_or("no detail")
        ),
    )
}

/// What the generator baselines found besides clean searches.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct BaselineFound {
    /// Confirmed `Active` violations.
    pub(super) findings: Vec<PropertyFinding>,
    /// Every other search that wasn't clean.
    pub(super) failures: Vec<GeneratorBaselineFailure>,
}

/// The class a pair's lanes get from its unpatched search, `None` for a
/// clean one, and the baseline failure to report, if any. `confirmation` is
/// a violation's fresh confirmation. A confirmed violation is a finding,
/// and its lanes `baseline-fails` with no reason. A violation that didn't
/// confirm, and any other failure, is a baseline failure, and its lanes
/// `baseline-fails` with its reason.
pub(super) fn baseline_lane(
    unpatched: &Unpatched,
    confirmation: Option<&FollowUp>,
) -> (Option<Classified>, Option<Classified>) {
    let failed = |result: Classified| {
        let lane = Classified::new(Class::BaselineFails, result.reason, result.detail.clone());
        (Some(lane), Some(result))
    };
    match unpatched {
        Unpatched::Clean => (None, None),
        Unpatched::Violation(id) => {
            let violates = format!("the unpatched search violates {id}");
            match confirmation {
                Some(confirmation) if confirmation.ok => (
                    Some(Classified::new(Class::BaselineFails, None, violates)),
                    None,
                ),
                _ => failed(confirmed(
                    Classified::new(Class::HarnessFailure, None, violates),
                    confirmation.unwrap_or(&FollowUp::failed("no confirmation ran".to_owned())),
                )),
            }
        }
        Unpatched::Failed { result, .. } => failed(result.clone()),
    }
}

impl Runner {
    /// Sets the generator lanes up on slot 0's unpatched `built`, and runs
    /// each harness and seed's baseline search. Returns the lanes, the
    /// setup the report records, and the baselines' property findings and
    /// failures.
    pub(super) fn lanes(
        &self,
        cases: &[LoopCase],
        runnables: &[Runnable],
        slot: &Slot,
        built: &Built,
        source: &str,
    ) -> Result<(Lanes, GenerateConfig, BaselineFound), BoxError> {
        let flags = self.generate.clone();
        let sim = built
            .sim
            .as_ref()
            .ok_or("the unpatched build made no zakura-sim binary")?;
        let exclude = self.out.join(EXCLUDE_FILE);
        let excluded_inputs = write_exclusions(&slot.tree, &exclude)?;
        let corpus = self.out.join(EMPTY_CORPUS);
        fs::create_dir_all(&corpus)?;
        let unpatched_sim = self.out.join(UNPATCHED_SIM);
        fs::create_dir_all(unpatched_sim.parent().expect("the copy lies under --out"))?;
        fs::copy(sim, &unpatched_sim)?;
        let libfuzzer = flags.mode == GenerateMode::Libfuzzer;
        let targets = if libfuzzer {
            harness_targets(&slot.tree)?
        } else {
            BTreeMap::new()
        };
        let plans = plan(
            cases,
            runnables,
            &generator_map(sim)?,
            &no_generator(&slot.tree)?,
            flags.harness.as_deref(),
            libfuzzer.then_some(&targets),
        );
        for (case, plan) in cases.iter().zip(&plans) {
            if let Err(why) = plan {
                println!("{} {}: no generator lane: {why}", case.label, case.property);
            }
        }
        let config = GenerateConfig {
            mode: flags.mode,
            runs: flags.runs,
            harness_runs: flags.harness_runs.clone(),
            seeds: flags.seeds.clone(),
            harness: flags.harness.clone().unwrap_or_else(|| "auto".to_owned()),
            seconds: libfuzzer.then_some(flags.seconds),
            excluded_inputs,
        };
        let mut lanes = Lanes {
            flags,
            plans,
            baselines: BTreeMap::new(),
            exclude,
            corpus,
            targets,
            unpatched_sim,
        };
        let pairs: BTreeSet<(String, u64)> = lanes
            .plans
            .iter()
            .flatten()
            .flat_map(|harness| {
                lanes
                    .flags
                    .seeds
                    .iter()
                    .map(|seed| (harness.clone(), *seed))
            })
            .collect();
        let (baselines, found) = self.generator_baselines(slot, built, &lanes, &pairs, source)?;
        lanes.baselines = baselines;
        Ok((lanes, config, found))
    }

    /// Runs `harness`'s search on `seed` with `slot`'s binaries `built`,
    /// writing under `dir`, under `deadline` and the memory `cap`. Only
    /// records whose source digest is `source` count.
    #[allow(clippy::too_many_arguments)]
    fn search(
        &self,
        slot: &Slot,
        built: &Built,
        lanes: &Lanes,
        harness: &str,
        seed: u64,
        dir: &Path,
        source: &str,
        deadline: Duration,
        cap: Option<u64>,
    ) -> Result<Search, BoxError> {
        let started = Instant::now();
        let searched = match lanes.flags.mode {
            GenerateMode::Inproc => {
                let runnable = Runnable::Generate {
                    harness: harness.to_owned(),
                    seed,
                    runs: lanes.flags.runs_for(harness),
                    exclude: lanes.exclude.clone(),
                };
                Searched::Inproc(self.witness(slot, built, &runnable, dir, source, deadline, cap)?)
            }
            GenerateMode::Libfuzzer => {
                if dir.exists() {
                    fs::remove_dir_all(dir)?;
                }
                let sim = built
                    .sim
                    .as_ref()
                    .ok_or("the build made no zakura-sim binary")?;
                let name = format!("{}-{seed}", key(harness));
                let spec = LaneSpec {
                    target: &lanes.targets[harness],
                    harness,
                    name: &name,
                    seed: u32::try_from(seed)
                        .map_err(|_| format!("a libfuzzer seed fits a u32, and {seed} doesn't"))?,
                    corpus: &lanes.corpus,
                };
                // One worker per campaign, so a lane's memory is one
                // process's, as an `inproc` search's is.
                let bounds = CampaignBounds {
                    seconds_per_seed: lanes.flags.seconds,
                    workers: 1,
                    max_len: 512,
                    input_timeout_seconds: 60,
                    rss_limit_mb: cap.map_or(4096, |cap| cap >> 20),
                    max_restarts: 0,
                    abandon_limit: 1,
                    feedback: "on".to_owned(),
                    sanitizer: "none".to_owned(),
                };
                let make = || scoped(cap, sim);
                let deadline = deadline.max(lane::start_deadline(&bounds));
                Searched::Campaign(
                    lane::campaign_lane(
                        &make,
                        dir,
                        &slot.target.join("fuzz"),
                        &spec,
                        &bounds,
                        deadline,
                    )?
                    .map_err(lane_failure),
                )
            }
        };
        Ok(Search {
            searched,
            millis: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            dir: dir.to_owned(),
        })
    }

    /// Confirms `kill` on `harness` as a kill ([`Confirm::Kill`]) in fresh
    /// processes with `built`'s simulator, allowing the `also` IDs, and
    /// shrinks the confirmed trace. A search's run took `per_run`, which
    /// sets each child's deadline ([`confirm_child`]) and the shrink's wall
    /// ([`shrink_wall`]). Writes under `dir`. Returns the confirmation, and
    /// the shrink if the kill confirmed.
    #[allow(clippy::too_many_arguments)]
    fn follow_up(
        &self,
        built: &Built,
        harness: &str,
        kill: &Kill,
        also: &[String],
        dir: &Path,
        cap: Option<u64>,
        per_run: Duration,
    ) -> Result<(FollowUp, Option<FollowUp>), BoxError> {
        let wall = shrink_wall(per_run);
        let Some(input) = &kill.input else {
            return Ok((
                FollowUp::failed("the search saved no input".to_owned()),
                None,
            ));
        };
        let sim = built
            .sim
            .as_ref()
            .ok_or("the build made no zakura-sim binary")?;
        let make = || scoped(cap, sim);
        let confirmed = confirm_and_replay(
            &make,
            harness,
            input,
            &kill.id,
            also,
            Confirm::Kill,
            &dir.join("confirmation"),
            confirm_child(per_run),
        )?;
        let (trace, missing_witness) = match confirmed {
            Ok(confirmed) => (confirmed.dir, confirmed.missing_witness),
            Err(why) => return Ok((FollowUp::failed(why), None)),
        };
        let confirmation = FollowUp {
            ok: true,
            trace: Some(self.relative(&trace)),
            detail: missing_witness,
        };
        let out = dir.join("shrink");
        let log = dir.join("shrink.log");
        let mut shrink = make();
        shrink
            .arg("shrink")
            .arg(&trace)
            .arg("--out")
            .arg(&out)
            .args([
                "--candidates",
                &SHRINK_CANDIDATES.to_string(),
                "--wall-secs",
                &wall.as_secs().to_string(),
            ])
            .env(EXPECT_VAR, &kill.id);
        let reduced = out.join("reduced");
        let shrunk = match run_lane_child(&mut shrink, &log, Some(wall + MIN_DEADLINE))? {
            ChildEnd::Exited(0) if reduced.is_dir() => FollowUp {
                ok: true,
                trace: Some(self.relative(&reduced)),
                detail: None,
            },
            end => FollowUp::failed(format!(
                "`zakura-sim shrink` {end}; see {}",
                self.relative(&log).display()
            )),
        };
        Ok((confirmation, Some(shrunk)))
    }

    /// Runs the kill's `which` trace, at `trace`, on `harness`'s unpatched
    /// build, under the memory `cap` and within `child`, and judges whether
    /// it violates `id` there. A trace with its `input.bin` reruns that
    /// input, and any other trace replays. Writes under `dir`.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn run_unpatched(
        &self,
        lanes: &Lanes,
        harness: &str,
        which: KillTrace,
        trace: &Path,
        id: &str,
        dir: &Path,
        cap: Option<u64>,
        child: Duration,
    ) -> Result<UnpatchedCheck, BoxError> {
        let input = trace.join("input.bin");
        // A mutant's trace holds the mutant's picks, so the unpatched build
        // leaves them on most replays. A trace with its input reruns that
        // input from scratch instead. A shrunk trace has no input, so it
        // replays, and a replay that leaves the picks stays undecided.
        let rerun = input.is_file();
        let command = if rerun { "record" } else { "replay" };
        let log = dir.join(format!("unpatched-{}-{command}.log", which.name()));
        let mut run = scoped(cap, &lanes.unpatched_sim);
        let report = if rerun {
            let out = dir.join(format!("unpatched-{}", which.name()));
            if out.exists() {
                fs::remove_dir_all(&out)?;
            }
            run.args(["record", "--sim", harness, "--input"])
                .arg(&input)
                .arg("--out")
                .arg(&out);
            out.join("full-report.json")
        } else {
            let report = dir.join(format!("unpatched-{}-report.json", which.name()));
            if report.exists() {
                fs::remove_file(&report)?;
            }
            // `--allow-build-mismatch` always exits 1, so only the report
            // counts.
            run.arg("replay")
                .arg(trace)
                .arg("--allow-build-mismatch")
                .arg("--report")
                .arg(&report);
            report
        };
        let ended = run_lane_child(&mut run, &log, Some(child))?;
        let written = fs::read_to_string(&report).ok();
        let (outcome, detail) =
            judge_unpatched(id, which, command, written.as_deref(), &ended.to_string());
        Ok(UnpatchedCheck {
            trace: which,
            outcome,
            detail,
            log: self.relative(&log),
        })
    }

    /// `path` relative to `--out`, or whole if it lies outside.
    fn relative(&self, path: &Path) -> PathBuf {
        path.strip_prefix(&self.out)
            .map_or_else(|_| path.to_owned(), Path::to_path_buf)
    }

    /// Runs each `(harness, seed)` search of `pairs` once on slot 0's
    /// unpatched `built`, then reruns alone, with the host's memory, each
    /// one a run failure ended. Returns each pair's baseline, and the
    /// property findings and baseline failures among them.
    fn generator_baselines(
        &self,
        slot: &Slot,
        built: &Built,
        lanes: &Lanes,
        pairs: &BTreeSet<Pair>,
        source: &str,
    ) -> Result<(BTreeMap<Pair, GenBaseline>, BaselineFound), BoxError> {
        let pending: Mutex<Vec<Pair>> = Mutex::new(pairs.iter().rev().cloned().collect());
        let ran: Mutex<BTreeMap<Pair, Search>> = Mutex::new(BTreeMap::new());
        let dir = |harness: &str, seed: u64, suffix: &str| {
            self.out
                .join("baseline")
                .join("generate")
                .join(format!("{}-seed-{seed}{suffix}", key(harness)))
        };
        // Campaigns build their fuzz target in the slot's one target
        // directory, so they run one at a time.
        let workers = match lanes.flags.mode {
            GenerateMode::Libfuzzer => 1,
            GenerateMode::Inproc => self.memory_max.map_or(1, |cap| {
                usize::try_from(host_memory().unwrap_or(cap) / cap.max(1)).unwrap_or(1)
            }),
        };
        thread::scope(|scope| {
            let handles: Vec<_> = (0..workers.max(1))
                .map(|_| {
                    scope.spawn(|| -> Result<(), String> {
                        loop {
                            let Some((harness, seed)) =
                                pending.lock().expect("no worker panics").pop()
                            else {
                                return Ok(());
                            };
                            let search = self
                                .search(
                                    slot,
                                    built,
                                    lanes,
                                    &harness,
                                    seed,
                                    &dir(&harness, seed, ""),
                                    source,
                                    BASELINE_DEADLINE,
                                    self.memory_max,
                                )
                                .map_err(|error| error.to_string())?;
                            ran.lock()
                                .expect("no worker panics")
                                .insert((harness, seed), search);
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
        let mut baselines = BTreeMap::new();
        let mut found = BaselineFound::default();
        for ((harness, seed), mut search) in ran.into_inner().expect("every worker joined") {
            let mut unpatched = search.unpatched();
            let rerun_alone = matches!(unpatched, Unpatched::Failed { rerun: true, .. });
            if rerun_alone {
                search = self.search(
                    slot,
                    built,
                    lanes,
                    &harness,
                    seed,
                    &dir(&harness, seed, "-alone"),
                    source,
                    BASELINE_DEADLINE,
                    self.alone_cap(),
                )?;
                unpatched = search.unpatched();
            }
            let confirmation = match &unpatched {
                Unpatched::Violation(id) => {
                    let kill = Kill {
                        input: search.input(id),
                        id: id.clone(),
                    };
                    let also = co_violations(&search.searched, id);
                    let done = self.follow_up(
                        built,
                        &harness,
                        &kill,
                        &also,
                        &search.dir.join(key(id)),
                        self.alone_cap(),
                        per_run(search.millis, search.runs()),
                    )?;
                    Some((kill, done))
                }
                _ => None,
            };
            let (failure, failed) =
                baseline_lane(&unpatched, confirmation.as_ref().map(|(_, (done, _))| done));
            println!(
                "generator baseline {harness} seed {seed}: {}",
                failure
                    .as_ref()
                    .map_or_else(|| "clean".to_owned(), |failure| failure.key())
            );
            let log = self.relative(&search.dir);
            if let Some(result) = failed {
                found.failures.push(GeneratorBaselineFailure {
                    harness: harness.clone(),
                    seed,
                    mode: lanes.flags.mode,
                    result,
                    rerun_alone,
                    log,
                });
            } else if let Some((kill, (confirmation, shrunk))) = confirmation {
                found.findings.push(PropertyFinding {
                    harness: harness.clone(),
                    seed,
                    mode: lanes.flags.mode,
                    detail: failure
                        .as_ref()
                        .map(|failure| failure.detail.clone())
                        .unwrap_or_default(),
                    property: Some(kill.id),
                    runs: search.runs(),
                    millis: search.millis,
                    input: kill.input.map(|input| self.relative(&input)),
                    family: search.family(),
                    confirmation: Some(confirmation),
                    shrunk,
                    log,
                });
            }
            baselines.insert(
                (harness, seed),
                GenBaseline {
                    failure,
                    millis: search.millis,
                },
            );
        }
        Ok((baselines, found))
    }

    /// The generator lanes of `cases` on `slot`'s patched `built`, by case
    /// index, writing under `dir`. Each harness and seed searches once, and
    /// each case on that harness is classified from the search. A kill is
    /// confirmed and shrunk once per search and violated ID.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn generator_lanes(
        &self,
        slot: &Slot,
        built: &Built,
        lanes: &Lanes,
        cases: &[(usize, &LoopCase)],
        dir: &Path,
        source: &str,
        cap: Option<u64>,
    ) -> Result<BTreeMap<usize, Vec<GeneratorReport>>, BoxError> {
        let mut by_harness: BTreeMap<&str, Vec<(usize, &LoopCase)>> = BTreeMap::new();
        for (index, case) in cases {
            if let Ok(harness) = &lanes.plans[*index] {
                by_harness.entry(harness).or_default().push((*index, case));
            }
        }
        let mode = lanes.flags.mode;
        let mut reports: BTreeMap<usize, Vec<GeneratorReport>> = BTreeMap::new();
        for (harness, cases) in by_harness {
            for seed in &lanes.flags.seeds {
                let baseline = &lanes.baselines[&(harness.to_owned(), *seed)];
                if let Some(result) = &baseline.failure {
                    for (index, _) in &cases {
                        reports
                            .entry(*index)
                            .or_default()
                            .push(GeneratorReport::unsearched(
                                harness,
                                *seed,
                                mode,
                                result.clone(),
                            ));
                    }
                    continue;
                }
                let search_dir = dir
                    .join("generate")
                    .join(format!("{}-seed-{seed}", key(harness)));
                let deadline =
                    (Duration::from_millis(baseline.millis) * DEADLINE_FACTOR).max(MIN_DEADLINE);
                let search = self.search(
                    slot,
                    built,
                    lanes,
                    harness,
                    *seed,
                    &search_dir,
                    source,
                    deadline,
                    cap,
                )?;
                let run_time = per_run(baseline.millis, Some(lanes.flags.runs_for(harness)));
                let mut follow_ups: BTreeMap<
                    String,
                    (FollowUp, Option<FollowUp>, Vec<UnpatchedCheck>),
                > = BTreeMap::new();
                for (index, case) in &cases {
                    let (mut result, kill) = search.classify(&case.property, &case.also_violates);
                    let mut lane = GeneratorReport {
                        runs: search.runs(),
                        millis: Some(search.millis),
                        log: Some(self.relative(&search_dir)),
                        ..GeneratorReport::unsearched(harness, *seed, mode, result.clone())
                    };
                    if let Some(kill) = kill {
                        if !follow_ups.contains_key(&kill.id) {
                            let also = co_violations(&search.searched, &kill.id);
                            let kill_dir = search_dir.join(key(&kill.id));
                            let (confirmation, shrunk) = self.follow_up(
                                built, harness, &kill, &also, &kill_dir, cap, run_time,
                            )?;
                            let traces = [
                                (KillTrace::Confirmed, Some(&confirmation)),
                                (KillTrace::Shrunk, shrunk.as_ref()),
                            ];
                            let mut checks = Vec::new();
                            for (which, follow_up) in traces {
                                if let Some(FollowUp {
                                    ok: true,
                                    trace: Some(trace),
                                    ..
                                }) = follow_up
                                {
                                    checks.push(self.run_unpatched(
                                        lanes,
                                        harness,
                                        which,
                                        &self.out.join(trace),
                                        &kill.id,
                                        &kill_dir,
                                        cap,
                                        confirm_child(run_time),
                                    )?);
                                }
                            }
                            follow_ups.insert(kill.id.clone(), (confirmation, shrunk, checks));
                        }
                        let (confirmation, shrunk, checks) = &follow_ups[&kill.id];
                        result = unpatched_class(confirmed(result, confirmation), checks);
                        lane.confirmation = Some(confirmation.clone());
                        lane.shrunk.clone_from(shrunk);
                        lane.unpatched.clone_from(checks);
                        lane.input = kill.input.map(|input| self.relative(&input));
                        lane.family = search.family();
                        lane.violated = Some(kill.id);
                    }
                    println!(
                        "{} {} generator {harness} seed {seed}: {}: {}",
                        case.label,
                        case.property,
                        result.key(),
                        result.detail
                    );
                    lane.result = result;
                    reports.entry(*index).or_default().push(lane);
                }
            }
        }
        Ok(reports)
    }
}
