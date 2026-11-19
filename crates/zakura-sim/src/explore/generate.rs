//! A deterministic in-process generator: draws fuzz inputs from a seed,
//! runs each on one harness, and stops at the first `Active` violation
//! (mutation plan, mut-4.1).
//!
//! [`inputs`] draws the inputs from a SplitMix64 stream that the seed
//! fixes, so a seed gives the same inputs on every run and every build.
//! [`generate`] runs them in order on a [`Worker`], classifies each run by
//! the fuzz targets' rules ([`classify`](super::classify)), and stops at
//! the first run that a fuzz target would crash on:
//!
//! - an `Active` violation;
//! - a harness failure;
//! - the abandonment limit ([`super::abandon_limit`]);
//! - a panic outside any task.
//!
//! A `Known` or `Draft` violation is counted under
//! [`GenerateReport::notes`], and the search goes on. An input whose
//! SHA-256 is in the exclusion set, such as a committed witness's, is
//! skipped without a run.
//!
//! The search holds the end-record hook ([`end_records::hold`]), so its
//! runs write no records. Once it stops, it saves the stopping run's input
//! in the output directory and writes that run's end record, and only
//! that one, to the record directory. A search that runs out of inputs
//! writes no record.
//!
//! The report counts each property's outcomes over the runs, so a reader
//! can tell a property that held from one that never applied.
//!
//! A harness whose draw picks scenario families by a tag at the prefix's
//! start ([`HarnessEntry::families`]) would almost never see a tagged
//! family from uniform bytes. [`draws`] therefore picks, for each run, one
//! of the families or no tag, uniformly, from the seed and the run's index,
//! and writes the family's tag over the input's first bytes. The input
//! stays the whole run, so replay, shrink and confirm read it unchanged.
//!
//! A uniform pick repeats families long before it has drawn them all, so a
//! slow harness's short search misses most of them. [`DrawPolicy::Cycle`]
//! instead visits every family, each of its variants and one untagged run
//! once per block of runs ([`draws_with`]).
//!
//! A planned family reaches only the states someone named. A free family
//! ([`ScenarioFamily::free`]) composes the scenario from its bytes: the
//! adversary's actions, the node's limits and the links' faults.
//! [`DrawPolicy::Free`] draws only free families, and [`DrawPolicy::Mix`]
//! takes turns: a cycled family under the Plan, a cycled family under PCT,
//! and a free draw. The report counts each property's outcomes over the free
//! runs apart ([`GenerateReport::random_properties`]), so a reader can tell
//! a property that only a planned family reaches.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs, io,
    panic::{self, AssertUnwindSafe},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, PoisonError},
    time::Instant,
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{
    enumerate::{self, Delays, Enumeration},
    pct::Pct,
    Bucket, Classified, ExploreOutcome, ExploreRun, Worker,
};
use crate::{
    check::PropertyOutcome,
    harness::{HarnessEntry, ScenarioFamily, HARNESSES},
    record::EndRecord,
    trace::{end_records, meta::hex},
};

/// The file [`generate`] writes its report to, in its output directory.
pub const REPORT_FILE: &str = "generate.json";

/// The name [`draws`] gives a run on which it wrote no family's tag.
pub const UNTAGGED: &str = "untagged";

/// The longest input [`inputs`] draws by default: the pilot's discovery
/// campaigns' `max_len`.
pub const DEFAULT_MAX_BYTES: usize = 512;

/// What a search runs.
#[derive(Clone, Debug)]
pub struct GenerateOptions {
    /// The seed of [`inputs`].
    pub seed: u64,
    /// How many inputs to draw. Each excluded input counts.
    pub runs: u64,
    /// The longest input to draw.
    pub max_bytes: usize,
    /// The SHA-256 digests, in lowercase hex, of the drawn inputs to skip:
    /// a report's [`GenerateReport::draw_sha256`], or its `input_sha256`
    /// when it has none.
    pub exclude: BTreeSet<String>,
    /// Where the report and the stopping run's input go.
    pub out: PathBuf,
    /// Where the stopping run's end record goes, if anywhere.
    pub records: Option<PathBuf>,
    /// How many abandoned runs the search takes before it stops.
    pub abandon_limit: usize,
    /// How each run's scenario family is drawn.
    pub draw_policy: DrawPolicy,
    /// What makes each run's live picks.
    pub schedule: Schedule,
    /// The families the search draws, by name, or every family and
    /// untagged runs if empty. A draw of another family, or an untagged
    /// one, is passed over and counts as no run.
    pub families: BTreeSet<String>,
}

/// What makes a search's live picks.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Schedule {
    /// The input's Plan bytes, then round-robin once they run out.
    #[default]
    Plan,
    /// A PCT steer the input seeds ([`super::pct`]). The search saves the
    /// input that replays a stopping run with no steer, so every later
    /// command reads it as a plain input.
    Pct,
    /// Every schedule of the drawn input with at most
    /// [`enumerate::DEPTH`] delays, breadth first, one run each
    /// ([`super::enumerate`]). A search moves to the next drawn input once
    /// it has run every schedule of the current one. It saves a stopping
    /// run's input as [`Schedule::Pct`] does.
    Enumerate,
}

impl Schedule {
    /// True for [`Schedule::Plan`], which a report leaves out.
    fn is_plan(&self) -> bool {
        *self == Schedule::Plan
    }
}

/// How [`draws_with`] picks each run's scenario family.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DrawPolicy {
    /// Each run's family, or no tag, is uniform from the seed and the run's
    /// index alone, as before policies existed, so a seed keeps its runs.
    #[default]
    Uniform,
    /// Each block of runs visits every family's variants and one untagged
    /// run once, in an order the seed and the block shuffle. A search no
    /// longer than one block draws no slot twice.
    Cycle,
    /// Each run draws a free family ([`ScenarioFamily::free`]), cycling
    /// through their variants as [`DrawPolicy::Cycle`] does. A harness
    /// with no free family draws untagged runs, which are free only if its
    /// untagged prefix is ([`HarnessEntry::untagged_free`]).
    Free,
    /// Runs take turns: run `3k` draws a cycled family under the Plan, run
    /// `3k + 1` a cycled family under PCT, and run `3k + 2` a free draw,
    /// under PCT for odd `k` and the Plan for even `k`. The cycled runs
    /// visit every slot once per block of their own, and the free runs
    /// likewise. A harness the scheduler doesn't run takes the Plan for
    /// each PCT turn.
    Mix,
}

impl DrawPolicy {
    /// True for [`DrawPolicy::Uniform`], which a report leaves out.
    fn is_uniform(&self) -> bool {
        *self == DrawPolicy::Uniform
    }
}

/// One drawn input of a search ([`planned_draws`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Draw {
    /// The family whose tag starts the input, [`UNTAGGED`], or `None` on a
    /// harness without families.
    pub family: Option<&'static str>,
    /// True if the input's scenario is free: its family is free, or it is
    /// untagged on a harness whose untagged prefix is free.
    pub free: bool,
    /// The run's schedule. Only [`DrawPolicy::Mix`] varies it; every other
    /// policy leaves it to the search's own.
    pub schedule: Option<Schedule>,
    /// The input.
    pub input: Vec<u8>,
}

impl Draw {
    /// The run's mode as a report names it, such as `cycle/pct`, for a
    /// [`DrawPolicy::Mix`] draw.
    fn mode(&self, schedule: Schedule) -> String {
        let draw = if self.free { "free" } else { "cycle" };
        let schedule = match schedule {
            Schedule::Plan => "plan",
            Schedule::Pct => "pct",
            Schedule::Enumerate => "enumerate",
        };
        format!("{draw}/{schedule}")
    }
}

/// Why a search stopped.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum Stop {
    /// Every drawn input ran, and none stopped the search.
    Clean,
    /// A run violated an `Active` property.
    Violation {
        /// The violated property.
        property: String,
    },
    /// A run ended at a harness failure, or the harness couldn't run it.
    HarnessFailure {
        /// The failure.
        reason: String,
    },
    /// Shutdown abandoned the search's limit of runs.
    Abandoned {
        /// Tasks the last abandoned run leaked.
        leaked: usize,
    },
    /// A run panicked outside any task.
    Panic {
        /// The panic's message, if it had a text one.
        message: String,
    },
}

/// One property's outcomes over a search's runs. A run that ended before
/// the property reported counts nowhere.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Applicability {
    /// Runs where it held with at least one applicable evaluation.
    pub held: u64,
    /// Runs where it was violated.
    pub violated: u64,
    /// Runs where it ended not-applicable with incomplete observations.
    pub incomplete: u64,
    /// Runs where nothing was judged: a benign not-applicable reason, or a
    /// hold with no applicable evaluation.
    pub not_applicable: u64,
}

impl Applicability {
    /// True if some run applied the property: it held with an applicable
    /// evaluation, or was violated.
    pub fn applied(&self) -> bool {
        self.held > 0 || self.violated > 0
    }

    /// Counts `outcome`.
    fn count(&mut self, outcome: &PropertyOutcome) {
        let counter = match outcome {
            PropertyOutcome::Held { applicable } if *applicable > 0 => &mut self.held,
            PropertyOutcome::NotApplicable { reason } if !reason.is_benign() => {
                &mut self.incomplete
            }
            PropertyOutcome::Held { .. } | PropertyOutcome::NotApplicable { .. } => {
                &mut self.not_applicable
            }
            PropertyOutcome::Violated { .. } => &mut self.violated,
        };
        *counter += 1;
    }
}

/// What a search did.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GenerateReport {
    /// The harness's name.
    pub harness: String,
    /// The harness's generator version.
    pub generator: u32,
    /// The seed of [`inputs`].
    pub seed: u64,
    /// How many inputs the search could draw.
    pub runs: u64,
    /// The longest input it could draw.
    pub max_bytes: usize,
    /// How each run's scenario family was drawn. Left out for
    /// [`DrawPolicy::Uniform`], so earlier reports read the same.
    #[serde(default, skip_serializing_if = "DrawPolicy::is_uniform")]
    pub draw_policy: DrawPolicy,
    /// What made each run's live picks. Left out for [`Schedule::Plan`].
    #[serde(default, skip_serializing_if = "Schedule::is_plan")]
    pub schedule: Schedule,
    /// Inputs it ran.
    pub executed: u64,
    /// Inputs it skipped as excluded.
    pub skipped: u64,
    /// How long the search took.
    pub millis: u64,
    /// Why it stopped.
    pub stop: Stop,
    /// The index in the draw order of the input that stopped the search,
    /// counting skipped inputs. `None` for a clean search.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kill_run: Option<u64>,
    /// The `Active` property whose violation stopped the search.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub violated: Option<String>,
    /// The saved input that stopped the search.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input: Option<PathBuf>,
    /// Its SHA-256.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_sha256: Option<String>,
    /// The SHA-256 of the drawn input whose run stopped the search, which
    /// `--exclude` takes. Left out when it is `input_sha256`: a steered run
    /// saves its replay, whose bytes differ from its draw.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub draw_sha256: Option<String>,
    /// The scenario family whose tag starts that input, or [`UNTAGGED`].
    /// `None` for a harness without families, or a clean search.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub family: Option<String>,
    /// Inputs run in each scenario family, and [`UNTAGGED`], for a harness
    /// with families.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub families: BTreeMap<String, u64>,
    /// The stopping run's mode, such as `free/pct`, under
    /// [`DrawPolicy::Mix`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
    /// Inputs run in each mode, under [`DrawPolicy::Mix`].
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub modes: BTreeMap<String, u64>,
    /// Inputs run whose scenario was free ([`Draw::free`]).
    #[serde(default)]
    pub random_runs: u64,
    /// Drawn inputs whose every schedule ran, under
    /// [`Schedule::Enumerate`].
    #[serde(default, skip_serializing_if = "is_zero")]
    pub exhausted: u64,
    /// True if the stopping run's end record went to the record directory.
    pub record: bool,
    /// Runs that violated each `Known` or `Draft` property, by bucket and
    /// property, such as `known` and `discovery/x`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub notes: BTreeMap<String, BTreeMap<String, u64>>,
    /// Runs whose shutdown leaked tasks.
    pub abandoned: u64,
    /// Each property's outcomes over the runs.
    pub properties: BTreeMap<String, Applicability>,
    /// Each property's outcomes over the free runs alone: the reach that
    /// no planned family gave it.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub random_properties: BTreeMap<String, Applicability>,
}

/// The inputs a search with `seed` draws, in order: each is up to
/// `max_bytes` long, with its length and bytes from one SplitMix64 stream
/// that `seed` starts.
pub fn inputs(seed: u64, max_bytes: usize) -> impl Iterator<Item = Vec<u8>> {
    let mut state = seed;
    let mut next = move || {
        state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    };
    // A usize always fits in a u64 on supported targets.
    let span = max_bytes as u64 + 1;
    std::iter::repeat_with(move || {
        // The remainder is at most `max_bytes`, a usize.
        let len = (next() % span) as usize;
        // Each byte is the low 8 bits of a draw.
        (0..len).map(|_| next() as u8).collect()
    })
}

/// The inputs a search with `seed` draws on a harness with `families`, in
/// order, each with its family's name. Each is [`inputs`]' input with one
/// family's tag written over its first bytes, extending a shorter input, or
/// unchanged and named [`UNTAGGED`]. The family is uniform over `families`
/// and no tag, from `seed` and the input's index alone. Without families,
/// every input is [`inputs`]' and has no name.
pub fn draws(
    seed: u64,
    max_bytes: usize,
    families: Vec<ScenarioFamily>,
) -> impl Iterator<Item = (Option<&'static str>, Vec<u8>)> {
    draws_with(seed, max_bytes, families, DrawPolicy::Uniform)
}

/// [`draws`] under `policy`. [`DrawPolicy::Uniform`] gives [`draws`]'
/// inputs. [`DrawPolicy::Cycle`] gives each input one slot: a family with
/// a value of the byte after its tag below its variant count, which it
/// writes there when the family has more than one variant, or no tag. Run
/// `index` takes a slot of block `index / slots`, whose runs visit every
/// slot once ([`cycled`]). [`DrawPolicy::Free`] and [`DrawPolicy::Mix`]
/// draw as [`planned_draws`] says, on a harness whose untagged prefix isn't
/// free. Either way the bytes past what the slot writes are [`inputs`]'
/// own.
pub fn draws_with(
    seed: u64,
    max_bytes: usize,
    families: Vec<ScenarioFamily>,
    policy: DrawPolicy,
) -> impl Iterator<Item = (Option<&'static str>, Vec<u8>)> {
    planned_draws(seed, max_bytes, families, false, policy).map(|draw| (draw.family, draw.input))
}

/// The slots a cycle over `families` visits: each family's variants, then
/// no tag if `untagged`.
fn slots(families: &[ScenarioFamily], untagged: bool) -> Vec<Option<(usize, u8)>> {
    families
        .iter()
        .enumerate()
        .flat_map(|(family, listed)| {
            (0..listed.variants).map(move |variant| {
                Some((
                    family,
                    u8::try_from(variant).expect("a family has at most 256 variants"),
                ))
            })
        })
        .chain(untagged.then_some(None))
        .collect()
}

/// The draws of a search with `seed` on a harness with `families`, whose
/// untagged prefix is free if `untagged_free`, under `policy`, in order.
///
/// - [`DrawPolicy::Uniform`] and [`DrawPolicy::Cycle`] give [`draws_with`]'
///   inputs.
/// - [`DrawPolicy::Free`] cycles through the free families' slots, or
///   draws untagged runs on a harness with none.
/// - [`DrawPolicy::Mix`] takes turns, and gives each draw its schedule:
///   two cycled draws, the first under the Plan and the second under PCT,
///   then one free draw, under PCT on every other turn. The cycled draws
///   take their slots from their own count, as do the free draws, so each
///   kind visits all of its slots once per block of its own.
///
/// A draw is free if its family is free, or it is untagged and
/// `untagged_free`. The bytes past what a slot writes are [`inputs`]' own.
pub fn planned_draws(
    seed: u64,
    max_bytes: usize,
    families: Vec<ScenarioFamily>,
    untagged_free: bool,
    policy: DrawPolicy,
) -> impl Iterator<Item = Draw> {
    // A usize always fits in a u64 on supported targets.
    let choices = families.len() as u64 + 1;
    let every = slots(&families, true);
    let free: Vec<Option<(usize, u8)>> = {
        let listed: Vec<Option<(usize, u8)>> = slots(&families, false)
            .into_iter()
            .filter(|slot| slot.is_some_and(|(family, _)| families[family].free))
            .collect();
        if listed.is_empty() {
            vec![None]
        } else {
            listed
        }
    };
    inputs(seed, max_bytes)
        .zip(0u64..)
        .map(move |(mut input, index)| {
            if families.is_empty() {
                let schedule = (policy == DrawPolicy::Mix).then(|| mix_schedule(index));
                return Draw {
                    family: None,
                    free: untagged_free,
                    schedule,
                    input,
                };
            }
            let (slot, schedule) = match policy {
                // The remainder is below `choices`, the families' count plus 1.
                DrawPolicy::Uniform => {
                    let pick = (mix(seed.rotate_left(32) ^ index) % choices) as usize;
                    ((pick < families.len()).then_some((pick, 0)), None)
                }
                DrawPolicy::Cycle => (every[cycled(seed, index, every.len())], None),
                DrawPolicy::Free => (free[cycled(seed ^ FREE_SALT, index, free.len())], None),
                DrawPolicy::Mix => {
                    let (turn, round) = (index % 3, index / 3);
                    let slot = if turn < 2 {
                        every[cycled(seed, 2 * round + turn, every.len())]
                    } else {
                        free[cycled(seed ^ FREE_SALT, round, free.len())]
                    };
                    (slot, Some(mix_schedule(index)))
                }
            };
            let slot = slot.map(|(family, variant)| {
                let listed = &families[family];
                let variant =
                    (policy != DrawPolicy::Uniform && listed.variants > 1).then_some(variant);
                (listed, variant)
            });
            match slot {
                Some((family, variant)) => {
                    let tag = family.tag.len();
                    let written = tag + usize::from(variant.is_some());
                    if input.len() < written {
                        input.resize(written, 0);
                    }
                    input[..tag].copy_from_slice(&family.tag);
                    if let Some(variant) = variant {
                        input[tag] = variant;
                    }
                    Draw {
                        family: Some(family.name),
                        free: family.free,
                        schedule,
                        input,
                    }
                }
                None => Draw {
                    family: Some(UNTAGGED),
                    free: untagged_free,
                    schedule,
                    input,
                },
            }
        })
}

/// Mixed into the seed of the free draws' cycle, so the free slots don't
/// follow the cycled slots' order.
const FREE_SALT: u64 = 0x4652_4545;

/// The schedule [`DrawPolicy::Mix`] gives run `index`: the Plan for the
/// first cycled turn, PCT for the second, and for the free turn PCT on odd
/// rounds and the Plan on even ones.
fn mix_schedule(index: u64) -> Schedule {
    match (index % 3, (index / 3) % 2) {
        (1, _) | (2, 1) => Schedule::Pct,
        _ => Schedule::Plan,
    }
}

/// The slot run `index` of a cycle draw takes, out of `count`. Run `index`
/// falls in block `index / count`, whose runs visit the slots in an order a
/// Fisher-Yates shuffle draws from `seed` and the block alone, so a run's
/// slot doesn't depend on which earlier inputs a search skipped.
fn cycled(seed: u64, index: u64, count: usize) -> usize {
    // A usize always fits in a u64 on supported targets.
    let span = count as u64;
    let mut order: Vec<usize> = (0..count).collect();
    let mut state = mix(seed.rotate_left(16) ^ (index / span));
    for last in (1..count).rev() {
        state = mix(state);
        // The remainder is at most `last`, a usize.
        order.swap(last, (state % (last as u64 + 1)) as usize);
    }
    // The remainder is below `span`, a usize.
    order[(index % span) as usize]
}

/// SplitMix64's finalizer: a well-mixed function of `x`.
fn mix(x: u64) -> u64 {
    let mut z = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// Reads an exclusion file: one SHA-256 in hex per line. Blank lines and
/// lines that start with `#` are skipped. Fails on any other line.
pub fn read_exclusions(text: &str) -> Result<BTreeSet<String>, String> {
    let mut digests = BTreeSet::new();
    for (number, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if line.len() != 64 || !line.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(format!("line {}: `{line}` isn't a SHA-256", number + 1));
        }
        digests.insert(line.to_ascii_lowercase());
    }
    Ok(digests)
}

/// Each registered property, with the harnesses that can search for its
/// violation: every harness in [`HARNESSES`] that reports it, except a
/// harness that plants its bugs ([`HarnessEntry::planted`]), whose runs
/// must violate its properties. In registry order.
pub fn generator_harnesses() -> BTreeMap<String, Vec<&'static str>> {
    let mut map: BTreeMap<String, Vec<&'static str>> = BTreeMap::new();
    for entry in HARNESSES.iter().filter(|entry| !entry.planted) {
        for property in (entry.properties)() {
            let harnesses = map.entry(property.id).or_default();
            if !harnesses.contains(&entry.name) {
                harnesses.push(entry.name);
            }
        }
    }
    map
}

/// What one run left for the search to count.
struct Ran {
    outcome: ExploreOutcome,
    notes: Vec<(String, Bucket)>,
    leaked: usize,
    properties: Vec<(String, PropertyOutcome)>,
    /// The run's end record, if the run stops the search.
    end: Option<EndRecord>,
    /// The input that replays the run with no steer, which the search
    /// saves if the run stops it: the drawn input itself, unless a steer
    /// made the picks.
    replay: Vec<u8>,
}

/// What `run` left for the search to count, with `replay` the input that
/// replays it.
fn summarize(replay: &[u8], run: &ExploreRun, classified: &Classified) -> Ran {
    let stops = !matches!(
        classified.outcome,
        ExploreOutcome::Clean | ExploreOutcome::Abandoned { .. }
    ) || classified.leaked > 0;
    Ran {
        outcome: classified.outcome.clone(),
        notes: classified.notes.clone(),
        leaked: classified.leaked,
        properties: run.report().map_or_else(Vec::new, |report| {
            report
                .properties
                .iter()
                .map(|report| (report.property.clone(), report.outcome.clone()))
                .collect()
        }),
        end: stops
            .then(|| run.report().map(|report| report.end_record()))
            .flatten(),
        replay: replay.to_vec(),
    }
}

/// Searches `entry`'s harness as `options` say, and writes the report to
/// [`REPORT_FILE`] in `options.out`.
///
/// Fails only if the output can't be written.
pub fn generate(
    entry: &'static HarnessEntry,
    options: &GenerateOptions,
) -> io::Result<GenerateReport> {
    fs::create_dir_all(&options.out)?;
    let _hold = end_records::hold();
    let started = Instant::now();
    let mut report = GenerateReport {
        harness: entry.name.to_owned(),
        generator: entry.generator,
        seed: options.seed,
        runs: options.runs,
        max_bytes: options.max_bytes,
        draw_policy: options.draw_policy,
        schedule: options.schedule,
        executed: 0,
        skipped: 0,
        millis: 0,
        stop: Stop::Clean,
        kill_run: None,
        violated: None,
        input: None,
        input_sha256: None,
        draw_sha256: None,
        family: None,
        families: BTreeMap::new(),
        mode: None,
        modes: BTreeMap::new(),
        random_runs: 0,
        exhausted: 0,
        record: false,
        notes: BTreeMap::new(),
        abandoned: 0,
        properties: BTreeMap::new(),
        random_properties: BTreeMap::new(),
    };
    let mut families = (entry.families)();
    if let Some(unknown) = options
        .families
        .iter()
        .find(|name| !families.iter().any(|family| family.name == name.as_str()))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{} has no family `{unknown}`", entry.name),
        ));
    }
    if !options.families.is_empty() {
        families.retain(|family| options.families.contains(family.name));
    }
    let mut worker = Worker::new();
    let mut stopped = None;
    let drawn = planned_draws(
        options.seed,
        options.max_bytes,
        families,
        entry.untagged_free,
        options.draw_policy,
    )
    .filter(|draw| {
        options.families.is_empty()
            || draw
                .family
                .is_some_and(|family| options.families.contains(family))
    });
    let mut drawn = drawn;
    // Under `Schedule::Enumerate`, the input whose schedules are running.
    let mut current: Option<(Draw, Enumeration)> = None;
    let enumerating = options.schedule == Schedule::Enumerate && entry.run_steered_bytes.is_some();
    for index in 0..options.runs {
        let (draw, delays) = if enumerating {
            let next = loop {
                if let Some((draw, enumeration)) = &mut current {
                    if let Some(delays) = enumeration.next_schedule() {
                        break Some((draw.clone(), delays));
                    }
                    report.exhausted += 1;
                }
                match drawn.next() {
                    Some(draw) => current = Some((draw, Enumeration::new(enumerate::DEPTH))),
                    None => break None,
                }
            };
            match next {
                Some((draw, delays)) => (draw, Some(delays)),
                None => break,
            }
        } else {
            match drawn.next() {
                Some(draw) => (draw, None),
                None => break,
            }
        };
        let family = draw.family;
        let digest = hex(&Sha256::digest(&draw.input));
        if options.exclude.contains(&digest) {
            report.skipped += 1;
            continue;
        }
        report.executed += 1;
        if let Some(family) = family {
            *report.families.entry(family.to_owned()).or_default() += 1;
        }
        // A harness the scheduler doesn't run has no picks to steer, so a
        // mixed search's PCT turns take the Plan there.
        let schedule = match draw.schedule.unwrap_or(options.schedule) {
            Schedule::Pct | Schedule::Enumerate if entry.run_steered_bytes.is_none() => {
                Schedule::Plan
            }
            schedule => schedule,
        };
        let mode = (options.draw_policy == DrawPolicy::Mix).then(|| draw.mode(schedule));
        if let Some(mode) = &mode {
            *report.modes.entry(mode.clone()).or_default() += 1;
        }
        report.random_runs += u64::from(draw.free);
        let input = draw.input.clone();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let ran = panic::catch_unwind(AssertUnwindSafe(|| match (schedule, &delays) {
            (Schedule::Pct, _) => worker.explore_steered(entry, &input, Pct::for_input, summarize),
            (Schedule::Enumerate, Some(delays)) => {
                let (delays, seen) = (delays.clone(), Arc::clone(&seen));
                worker.explore_steered(
                    entry,
                    &input,
                    move |_| -> Box<dyn crate::Steer> { Box::new(Delays::new(&delays, seen)) },
                    summarize,
                )
            }
            _ => worker.explore(entry, &input, summarize),
        }));
        if let (Some((_, enumeration)), Some(delays)) = (&mut current, &delays) {
            let counts = seen.lock().unwrap_or_else(PoisonError::into_inner);
            enumeration.expand(delays, &counts);
        }
        let ran = match ran {
            Ok(ran) => ran,
            Err(payload) => {
                let message = payload
                    .downcast_ref::<&str>()
                    .map(|text| (*text).to_owned())
                    .or_else(|| payload.downcast_ref::<String>().cloned())
                    .unwrap_or_default();
                stopped = Some((
                    index,
                    family,
                    mode,
                    input,
                    digest,
                    None,
                    Stop::Panic { message },
                    None,
                ));
                break;
            }
        };
        for (property, outcome) in &ran.properties {
            report
                .properties
                .entry(property.clone())
                .or_default()
                .count(outcome);
            if draw.free {
                report
                    .random_properties
                    .entry(property.clone())
                    .or_default()
                    .count(outcome);
            }
        }
        for (property, bucket) in &ran.notes {
            if matches!(bucket, Bucket::Known | Bucket::Draft) {
                *report
                    .notes
                    .entry(bucket.name().to_owned())
                    .or_default()
                    .entry(property.clone())
                    .or_default() += 1;
            }
        }
        report.abandoned += u64::from(ran.leaked > 0);
        let stop = match ran.outcome {
            ExploreOutcome::Fail { property } => Some(Stop::Violation { property }),
            ExploreOutcome::HarnessFailure { reason } => Some(Stop::HarnessFailure { reason }),
            ExploreOutcome::Abandoned { .. } | ExploreOutcome::Clean
                if worker.abandoned() >= options.abandon_limit =>
            {
                Some(Stop::Abandoned { leaked: ran.leaked })
            }
            ExploreOutcome::Abandoned { .. } | ExploreOutcome::Clean => None,
        };
        if let Some(stop) = stop {
            let saved = hex(&Sha256::digest(&ran.replay));
            let drawn = (saved != digest).then_some(digest);
            stopped = Some((index, family, mode, ran.replay, saved, drawn, stop, ran.end));
            break;
        }
    }
    if current
        .as_ref()
        .is_some_and(|(_, enumeration)| enumeration.is_done())
    {
        report.exhausted += 1;
    }
    if let Some((index, family, mode, input, digest, draw, stop, end)) = stopped {
        // The input goes to disk before anything else, as a fuzz target
        // saves its failures.
        let path = options.out.join(format!("{digest}.bin"));
        fs::write(&path, &input)?;
        if let (Some(dir), Some(end)) = (&options.records, &end) {
            end_records::write_end_to(dir, end);
            report.record = true;
        }
        if let Stop::Violation { property } = &stop {
            report.violated = Some(property.clone());
        }
        report.kill_run = Some(index);
        report.input = Some(path);
        report.input_sha256 = Some(digest);
        report.draw_sha256 = draw;
        report.family = family.map(str::to_owned);
        report.mode = mode;
        report.stop = stop;
    }
    report.millis = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    let text = serde_json::to_string_pretty(&report).map_err(io::Error::other)?;
    write_atomically(
        &options.out.join(REPORT_FILE),
        format!("{text}\n").as_bytes(),
    )?;
    Ok(report)
}

/// True for zero, which a report leaves out.
fn is_zero(count: &u64) -> bool {
    *count == 0
}

/// Writes `bytes` to a temporary file beside `path`, then renames it to
/// `path`, so a reader never sees half a report.
fn write_atomically(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let partial = path.with_extension("partial");
    fs::write(&partial, bytes)?;
    fs::rename(&partial, path)
}
