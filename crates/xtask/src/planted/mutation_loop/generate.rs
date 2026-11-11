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
//! - **Baselines.** One search per harness and seed runs on the unpatched
//!   build first. A search that stops there is a property finding: ordinary
//!   property testing that found a bug on the pinned revision, or a
//!   property defect. The report lists it under `property_findings`, and
//!   that harness and seed's lane is `baseline-fails` for every case. The
//!   witness lane doesn't depend on it.
//! - **Exclusions.** Every search skips each committed input under
//!   [`EXCLUDED_DIRS`], by SHA-256, so it can't rediscover a witness.
//! - **Classes.** A lane gets the witness lane's classes
//!   ([`classify::generated`]).
//! - **Confirmation.** A kill's input must violate the same ID in a fresh
//!   `zakura-sim confirm`, and its trace must replay Exact. Otherwise the
//!   lane is a harness failure (`unconfirmed`). The loop then shrinks the
//!   confirmed trace with `zakura-sim shrink` under `ZAKURA_SIM_EXPECT`,
//!   within [`SHRINK_CANDIDATES`] and [`SHRINK_WALL`].
//!
//! A mutant's search runs once per harness and seed, and the loop
//! classifies each of the mutant's cases from it. A search runs under the
//! witness lane's memory cap. Its deadline is its baseline's time times the
//! witness lane's factor, and at least the witness lane's minimum.

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
        self, confirm_and_replay, run_lane_child, BuildFailure, CampaignBounds, CampaignFacts,
        LaneError, LaneSpec, EXPECT_VAR,
    },
    classify::{self, Class, Classified, Ended, GenerateFacts, GenerateStop, Reason, Record},
    host_memory, key, scoped, Built, LoopCase, Ran, Runnable, Runner, Slot, BASELINE_DEADLINE,
    DEADLINE_FACTOR, MIN_DEADLINE, WORK_LIST,
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

/// The most candidates a kill's shrink tries.
const SHRINK_CANDIDATES: u32 = 500;

/// The longest a kill's shrink runs.
const SHRINK_WALL: Duration = Duration::from_secs(120);

/// The longest one confirm or replay child runs: the pilot catalogue's
/// `child_seconds`.
const CONFIRM_CHILD: Duration = Duration::from_secs(300);

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
}

impl Default for GenerateFlags {
    fn default() -> Self {
        GenerateFlags {
            runs: 0,
            seeds: DEFAULT_SEEDS.to_vec(),
            harness: None,
            mode: GenerateMode::Inproc,
            seconds: DEFAULT_SECONDS,
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
}

/// The generator lanes' setup, as the report records it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(in crate::planted) struct GenerateConfig {
    pub(in crate::planted) mode: GenerateMode,
    /// The runs an `inproc` search draws.
    pub(in crate::planted) runs: u64,
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
    /// Why the step failed.
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
    /// The search's evidence, relative to `--out`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(in crate::planted) log: Option<PathBuf>,
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
            log: None,
        }
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

/// A harness and seed's unpatched search.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct GenBaseline {
    /// Why the search isn't clean, or `None` if it is.
    pub(super) detail: Option<String>,
    pub(super) millis: u64,
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

    /// Why the search on the unpatched build isn't clean, or `None`, and
    /// the ID whose violation stopped it, if one did.
    pub(super) fn baseline(&self) -> (Option<String>, Option<String>) {
        match &self.searched {
            Searched::Inproc(ran) => {
                let facts = ran.generated.as_ref();
                let violated = match facts {
                    Some(GenerateFacts {
                        stop: GenerateStop::Violation { property },
                        ..
                    }) if ran.observed.ended == Ended::Exited(0) => Some(property.clone()),
                    _ => None,
                };
                (classify::generator_baseline(&ran.observed, facts), violated)
            }
            Searched::Campaign(Ok(campaign)) => {
                let facts = campaign.facts("");
                let detail = if let Some(id) = facts.violated.first() {
                    Some(format!("the campaign violates {id}"))
                } else if !facts.killed.is_empty() || !facts.failures.is_empty() {
                    let failures: Vec<&str> = facts
                        .killed
                        .iter()
                        .chain(&facts.failures)
                        .map(String::as_str)
                        .collect();
                    Some(format!(
                        "the campaign saved failures: {}",
                        failures.join(", ")
                    ))
                } else {
                    None
                };
                (detail, facts.violated.first().cloned())
            }
            Searched::Campaign(Err(failure)) => (Some(failure.detail.clone()), None),
        }
    }
}

/// A `libfuzzer` lane's class for `property`, allowing `also_violates`, by
/// the witness lane's rules. A worker that timed out or died, or a saved
/// failure that isn't a violation, is a harness failure. A violation of an
/// ID other than `property` and `also_violates` kills by another rule. A
/// violation of `property` kills by the named rule. A violation of
/// `also_violates` alone kills by another rule. A campaign without a
/// violation survived: `held` if `property` applied in a complete
/// process, else `not-applicable`.
pub(super) fn campaign_class(
    property: &str,
    also_violates: &[String],
    facts: &CampaignFacts,
) -> Classified {
    let harness = |reason, detail| Classified::new(Class::HarnessFailure, Some(reason), detail);
    let other = |id: &String| Classified {
        other: Some(id.clone()),
        ..Classified::new(
            Class::KilledByOtherRule,
            None,
            format!("the campaign violates {id} after {} runs", facts.runs),
        )
    };
    if !facts.killed.is_empty() {
        return harness(
            Reason::Timeout,
            format!(
                "the campaign's workers timed out or died: {}",
                facts.killed.join(", ")
            ),
        );
    }
    if !facts.failures.is_empty() {
        return harness(
            Reason::HarnessEnd,
            format!("the campaign saved failures: {}", facts.failures.join(", ")),
        );
    }
    if let Some(id) = facts
        .violated
        .iter()
        .find(|id| *id != property && !also_violates.contains(id))
    {
        return other(id);
    }
    if facts.violated.iter().any(|id| id == property) {
        return Classified::new(
            Class::KilledByNamedRule,
            None,
            format!("the campaign violates {property} after {} runs", facts.runs),
        );
    }
    if let Some(id) = facts.violated.first() {
        return other(id);
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

/// The other IDs that the run whose violation of `id` stopped `searched`
/// also violated, as its end record shows. The fresh confirmation allows
/// them.
fn co_violations(searched: &Searched, id: &str) -> Vec<String> {
    let Searched::Inproc(ran) = searched else {
        return Vec::new();
    };
    ran.observed
        .records
        .iter()
        .find_map(|record| match record {
            Record::End {
                stopped_by: Some(stopped),
                violated,
                ..
            } if stopped == id => Some(
                violated
                    .iter()
                    .filter(|other| *other != id)
                    .cloned()
                    .collect(),
            ),
            _ => None,
        })
        .unwrap_or_default()
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

impl Runner {
    /// Sets the generator lanes up on slot 0's unpatched `built`, and runs
    /// each harness and seed's baseline search. Returns the lanes, the
    /// setup the report records, and the baselines' property findings.
    pub(super) fn lanes(
        &self,
        cases: &[LoopCase],
        runnables: &[Runnable],
        slot: &Slot,
        built: &Built,
        source: &str,
    ) -> Result<(Lanes, GenerateConfig, Vec<PropertyFinding>), BoxError> {
        let flags = self.generate.clone();
        let sim = built
            .sim
            .as_ref()
            .ok_or("the unpatched build made no zakura-sim binary")?;
        let exclude = self.out.join(EXCLUDE_FILE);
        let excluded_inputs = write_exclusions(&slot.tree, &exclude)?;
        let corpus = self.out.join(EMPTY_CORPUS);
        fs::create_dir_all(&corpus)?;
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
        let (baselines, findings) =
            self.generator_baselines(slot, built, &lanes, &pairs, source)?;
        lanes.baselines = baselines;
        Ok((lanes, config, findings))
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
                    runs: lanes.flags.runs,
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
                Searched::Campaign(
                    lane::campaign_lane(sim, dir, &slot.target.join("fuzz"), &spec, &bounds)?
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

    /// Confirms `kill` on `harness` in fresh processes with `built`'s
    /// simulator, allowing the `also` IDs, and shrinks the confirmed trace.
    /// Writes under `dir`. Returns the confirmation, and the shrink if the
    /// kill confirmed.
    fn follow_up(
        &self,
        built: &Built,
        harness: &str,
        kill: &Kill,
        also: &[String],
        dir: &Path,
        cap: Option<u64>,
    ) -> Result<(FollowUp, Option<FollowUp>), BoxError> {
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
            &dir.join("confirmation"),
            CONFIRM_CHILD,
        )?;
        let trace = match confirmed {
            Ok(trace) => trace,
            Err(why) => return Ok((FollowUp::failed(why), None)),
        };
        let confirmation = FollowUp {
            ok: true,
            trace: Some(self.relative(&trace)),
            detail: None,
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
                &SHRINK_WALL.as_secs().to_string(),
            ])
            .env(EXPECT_VAR, &kill.id);
        let reduced = out.join("reduced");
        let shrunk = match run_lane_child(&mut shrink, &log, Some(SHRINK_WALL + MIN_DEADLINE))? {
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

    /// `path` relative to `--out`, or whole if it lies outside.
    fn relative(&self, path: &Path) -> PathBuf {
        path.strip_prefix(&self.out)
            .map_or_else(|_| path.to_owned(), Path::to_path_buf)
    }

    /// Runs each `(harness, seed)` search of `pairs` once on slot 0's
    /// unpatched `built`. Returns each pair's baseline and the property
    /// findings among them, each violation confirmed and shrunk.
    fn generator_baselines(
        &self,
        slot: &Slot,
        built: &Built,
        lanes: &Lanes,
        pairs: &BTreeSet<Pair>,
        source: &str,
    ) -> Result<(BTreeMap<Pair, GenBaseline>, Vec<PropertyFinding>), BoxError> {
        let pending: Mutex<Vec<(String, u64)>> = Mutex::new(pairs.iter().rev().cloned().collect());
        let ran: Mutex<BTreeMap<(String, u64), Search>> = Mutex::new(BTreeMap::new());
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
                            let dir = self
                                .out
                                .join("baseline")
                                .join("generate")
                                .join(format!("{}-seed-{seed}", key(&harness)));
                            let search = self
                                .search(
                                    slot,
                                    built,
                                    lanes,
                                    &harness,
                                    seed,
                                    &dir,
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
        let mut findings = Vec::new();
        for ((harness, seed), search) in ran.into_inner().expect("every worker joined") {
            let (detail, violated) = search.baseline();
            println!(
                "generator baseline {harness} seed {seed}: {}",
                detail.as_deref().unwrap_or("clean")
            );
            if let Some(detail) = &detail {
                let kill = violated.map(|id| Kill {
                    input: search.input(&id),
                    id,
                });
                findings.push(self.finding(built, lanes, &harness, seed, &search, detail, kill)?);
            }
            baselines.insert(
                (harness, seed),
                GenBaseline {
                    detail,
                    millis: search.millis,
                },
            );
        }
        Ok((baselines, findings))
    }

    /// The property finding of `harness`'s unpatched `search` on `seed`,
    /// which stopped for `detail`, at `kill` if a violation stopped it. A
    /// kill is confirmed and shrunk.
    #[allow(clippy::too_many_arguments)]
    fn finding(
        &self,
        built: &Built,
        lanes: &Lanes,
        harness: &str,
        seed: u64,
        search: &Search,
        detail: &str,
        kill: Option<Kill>,
    ) -> Result<PropertyFinding, BoxError> {
        let (confirmation, shrunk) = match &kill {
            Some(kill) => {
                let also = co_violations(&search.searched, &kill.id);
                let (confirmation, shrunk) = self.follow_up(
                    built,
                    harness,
                    kill,
                    &also,
                    &search.dir.join(key(&kill.id)),
                    self.memory_max,
                )?;
                (Some(confirmation), shrunk)
            }
            None => (None, None),
        };
        let (property, input) = match kill {
            Some(kill) => (Some(kill.id), kill.input),
            None => (None, None),
        };
        Ok(PropertyFinding {
            harness: harness.to_owned(),
            seed,
            mode: lanes.flags.mode,
            property,
            detail: detail.to_owned(),
            runs: search.runs(),
            millis: search.millis,
            input: input.map(|input| self.relative(&input)),
            family: search.family(),
            confirmation,
            shrunk,
            log: self.relative(&search.dir),
        })
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
                if let Some(detail) = &baseline.detail {
                    let result = Classified::new(Class::BaselineFails, None, detail.clone());
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
                let mut follow_ups: BTreeMap<String, (FollowUp, Option<FollowUp>)> =
                    BTreeMap::new();
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
                            let done = self.follow_up(
                                built,
                                harness,
                                &kill,
                                &also,
                                &search_dir.join(key(&kill.id)),
                                cap,
                            )?;
                            follow_ups.insert(kill.id.clone(), done);
                        }
                        let (confirmation, shrunk) = &follow_ups[&kill.id];
                        result = confirmed(result, confirmation);
                        lane.confirmation = Some(confirmation.clone());
                        lane.shrunk.clone_from(shrunk);
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
