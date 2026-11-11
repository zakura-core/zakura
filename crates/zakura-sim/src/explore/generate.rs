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

use std::{
    collections::{BTreeMap, BTreeSet},
    fs, io,
    panic::{self, AssertUnwindSafe},
    path::{Path, PathBuf},
    time::Instant,
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{Bucket, ExploreOutcome, Worker};
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
    /// The SHA-256 digests, in lowercase hex, of the inputs to skip.
    pub exclude: BTreeSet<String>,
    /// Where the report and the stopping run's input go.
    pub out: PathBuf,
    /// Where the stopping run's end record goes, if anywhere.
    pub records: Option<PathBuf>,
    /// How many abandoned runs the search takes before it stops.
    pub abandon_limit: usize,
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
    /// Runs where it ended inconclusive.
    pub inconclusive: u64,
    /// Runs where no evaluation applied, including a hold with none.
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
            PropertyOutcome::Held { .. } | PropertyOutcome::NotApplicable => {
                &mut self.not_applicable
            }
            PropertyOutcome::Inconclusive { .. } => &mut self.inconclusive,
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
    /// The scenario family whose tag starts that input, or [`UNTAGGED`].
    /// `None` for a harness without families, or a clean search.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub family: Option<String>,
    /// Inputs run in each scenario family, and [`UNTAGGED`], for a harness
    /// with families.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub families: BTreeMap<String, u64>,
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
    // A usize always fits in a u64 on supported targets.
    let choices = families.len() as u64 + 1;
    inputs(seed, max_bytes)
        .zip(0u64..)
        .map(move |(mut input, index)| {
            if families.is_empty() {
                return (None, input);
            }
            // The remainder is below `choices`, the families' count plus 1.
            match families.get((mix(seed.rotate_left(32) ^ index) % choices) as usize) {
                Some(family) => {
                    if input.len() < family.tag.len() {
                        input.resize(family.tag.len(), 0);
                    }
                    input[..family.tag.len()].copy_from_slice(&family.tag);
                    (Some(family.name), input)
                }
                None => (Some(UNTAGGED), input),
            }
        })
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
        executed: 0,
        skipped: 0,
        millis: 0,
        stop: Stop::Clean,
        kill_run: None,
        violated: None,
        input: None,
        input_sha256: None,
        family: None,
        families: BTreeMap::new(),
        record: false,
        notes: BTreeMap::new(),
        abandoned: 0,
        properties: BTreeMap::new(),
    };
    let mut worker = Worker::new();
    let mut stopped = None;
    let drawn = draws(options.seed, options.max_bytes, (entry.families)());
    for (index, (family, input)) in (0..options.runs).zip(drawn) {
        let digest = hex(&Sha256::digest(&input));
        if options.exclude.contains(&digest) {
            report.skipped += 1;
            continue;
        }
        report.executed += 1;
        if let Some(family) = family {
            *report.families.entry(family.to_owned()).or_default() += 1;
        }
        let ran = panic::catch_unwind(AssertUnwindSafe(|| {
            worker.explore(entry, &input, |_, run, classified| {
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
                }
            })
        }));
        let ran = match ran {
            Ok(ran) => ran,
            Err(payload) => {
                let message = payload
                    .downcast_ref::<&str>()
                    .map(|text| (*text).to_owned())
                    .or_else(|| payload.downcast_ref::<String>().cloned())
                    .unwrap_or_default();
                stopped = Some((index, family, input, digest, Stop::Panic { message }, None));
                break;
            }
        };
        for (property, outcome) in &ran.properties {
            report
                .properties
                .entry(property.clone())
                .or_default()
                .count(outcome);
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
            stopped = Some((index, family, input, digest, stop, ran.end));
            break;
        }
    }
    if let Some((index, family, input, digest, stop, end)) = stopped {
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
        report.family = family.map(str::to_owned);
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

/// Writes `bytes` to a temporary file beside `path`, then renames it to
/// `path`, so a reader never sees half a report.
fn write_atomically(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let partial = path.with_extension("partial");
    fs::write(&partial, bytes)?;
    fs::rename(&partial, path)
}
