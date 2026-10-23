//! The `meta` record, line 1 of every trace.

use std::sync::OnceLock;

use serde::{ser::SerializeMap, Deserialize, Serialize, Serializer};
use serde_json::Value;
use sha2::{Digest, Sha256};
use zakura_scheduler::SchedulerConfig;

use super::MAX_JSON_DEPTH;
use crate::RunConfig;

/// The random stream algorithm and its version, as `meta.rng` names it.
pub const RNG: &str = "splitmix64-v1";

/// What a build records when it can't read a value. A build with any unknown
/// provenance is never equivalent to another.
pub const UNKNOWN: &str = "unknown";

/// Everything needed to rebuild a run without its generator: the build, the
/// harness and its scenario, the limits, and the fixtures.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetaRecord {
    /// [`crate::record::FORMAT_VERSION`] when written.
    pub format: u32,
    /// [`crate::record::CONTRACT_VERSION`] when written.
    pub contract: u32,
    /// The harness's generator version: how input bytes become a scenario
    /// and picks.
    pub generator: u32,
    /// The build that wrote the trace.
    pub build: BuildInfo,
    /// The harness's name, such as `header-sync`.
    pub sim: String,
    /// The harness's scenario, in full. Written with sorted keys (see
    /// [`sorted`]).
    #[serde(serialize_with = "sorted")]
    pub scenario: Value,
    /// Each assumption the scenario declares, with its parameters.
    pub assumptions: Vec<AssumeRecord>,
    /// The scheduler's limits.
    pub scheduler: SchedulerRecord,
    /// The run's step limits.
    pub budget: BudgetRecord,
    /// The random stream, [`RNG`] today.
    pub rng: String,
    /// The blobs the scenario reads.
    pub fixtures: Vec<BlobRef>,
    /// Every property the run checks.
    pub properties: Vec<PropertyRef>,
    /// `input.bin`, when input bytes produced the run.
    pub input: Option<InputRef>,
    /// The trace this one was derived from, if any.
    pub derived_from: Option<Derivation>,
}

impl MetaRecord {
    /// The first field nested deeper than [`MAX_JSON_DEPTH`]: `scenario` or
    /// `assumptions`. Only these two fields take arbitrary JSON.
    pub fn too_deep_field(&self) -> Option<&'static str> {
        if deeper_than(&self.scenario, MAX_JSON_DEPTH) {
            Some("scenario")
        } else if self
            .assumptions
            .iter()
            .any(|a| deeper_than(&a.params, MAX_JSON_DEPTH))
        {
            Some("assumptions")
        } else {
            None
        }
    }

    /// The first field that holds a float: `scenario` or `assumptions`.
    /// Records hold no floats (traces.md, Records), and only these two
    /// fields take arbitrary JSON.
    pub fn float_field(&self) -> Option<&'static str> {
        if has_float(&self.scenario) {
            Some("scenario")
        } else if self.assumptions.iter().any(|a| has_float(&a.params)) {
            Some("assumptions")
        } else {
            None
        }
    }
}

/// True if `value` nests arrays and objects more than `limit` deep. A scalar
/// is depth 0 and `[0]` is depth 1. The walk stops past `limit`, so a value
/// of any depth can't overflow the stack here.
fn deeper_than(value: &Value, limit: usize) -> bool {
    let Some(inner) = limit.checked_sub(1) else {
        return matches!(value, Value::Array(_) | Value::Object(_));
    };
    match value {
        Value::Array(items) => items.iter().any(|item| deeper_than(item, inner)),
        Value::Object(map) => map.values().any(|item| deeper_than(item, inner)),
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => false,
    }
}

/// True if `value` holds a float at any depth. Callers check
/// [`deeper_than`] first, so the walk stays shallow.
fn has_float(value: &Value) -> bool {
    match value {
        Value::Number(number) => number.is_f64(),
        Value::Array(items) => items.iter().any(has_float),
        Value::Object(map) => map.values().any(has_float),
        Value::Null | Value::Bool(_) | Value::String(_) => false,
    }
}

/// Where a binary came from. Two builds are equivalent when their runs must
/// make the same observations (see [`BuildInfo::equivalent`]).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BuildInfo {
    /// `HEAD`'s commit, or [`UNKNOWN`].
    pub git: String,
    /// True if the work tree had any change, untracked files included, or if
    /// git couldn't say.
    pub dirty: bool,
    /// SHA-256 over every tracked and untracked file git doesn't ignore, so
    /// an unstaged edit changes it. [`UNKNOWN`] if it couldn't be read.
    pub source: String,
    /// SHA-256 of `Cargo.lock`.
    pub lockfile: String,
    /// `rustc --version`.
    pub rustc: String,
    /// The target triple.
    pub target: String,
    /// The target's cfg set, as `name` or `name="value"`, sorted. It
    /// includes `debug_assertions` and leaves out `sanitize`.
    pub cfg: Vec<String>,
    /// The crate's enabled features, sorted.
    pub features: Vec<String>,
    /// Cargo's `OPT_LEVEL`.
    pub opt_level: String,
    /// The sanitizer, such as `address`, if any.
    pub sanitizer: Option<String>,
    /// SHA-256 of the running executable, or [`UNKNOWN`].
    pub binary: String,
}

impl BuildInfo {
    /// This build's provenance, from the values `build.rs` compiled in, plus
    /// a hash of the running executable. The hash is read once per process.
    pub fn current() -> Self {
        static BINARY: OnceLock<String> = OnceLock::new();
        let binary = BINARY.get_or_init(|| {
            std::env::current_exe()
                .and_then(std::fs::read)
                .map_or_else(|_| UNKNOWN.to_owned(), |bytes| hex(&Sha256::digest(bytes)))
        });
        let sanitizer = env!("ZAKURA_SIM_BUILD_SANITIZER");
        BuildInfo {
            git: env!("ZAKURA_SIM_BUILD_GIT").to_owned(),
            dirty: env!("ZAKURA_SIM_BUILD_DIRTY") == "1",
            source: env!("ZAKURA_SIM_BUILD_SOURCE").to_owned(),
            lockfile: env!("ZAKURA_SIM_BUILD_LOCKFILE").to_owned(),
            rustc: env!("ZAKURA_SIM_BUILD_RUSTC").to_owned(),
            target: env!("ZAKURA_SIM_BUILD_TARGET").to_owned(),
            cfg: list(env!("ZAKURA_SIM_BUILD_CFG")),
            features: list(env!("ZAKURA_SIM_BUILD_FEATURES")),
            opt_level: env!("ZAKURA_SIM_BUILD_OPT_LEVEL").to_owned(),
            sanitizer: (!sanitizer.is_empty()).then(|| sanitizer.to_owned()),
            binary: binary.clone(),
        }
    }

    /// True if runs on `self` and `other` must make the same observations.
    ///
    /// That holds for the same executable, or for two clean builds of the
    /// same commit and source with the same lockfile, toolchain, target, cfg
    /// set and features. Opt level and sanitizer don't count, since they must
    /// not change observations, but `cfg` includes `debug_assertions`, so a
    /// debug build never matches a release build. A dirty build, or one with
    /// unknown provenance, matches only its own executable, since its
    /// recorded source may be stale.
    pub fn equivalent(&self, other: &Self) -> bool {
        if self.binary == other.binary && self.binary != UNKNOWN {
            return true;
        }
        self.clean()
            && other.clean()
            && self.git == other.git
            && self.source == other.source
            && self.lockfile == other.lockfile
            && self.rustc == other.rustc
            && self.target == other.target
            && self.cfg == other.cfg
            && self.features == other.features
    }

    /// True if the build's provenance is known and its tree was clean.
    fn clean(&self) -> bool {
        !self.dirty
            && [&self.git, &self.source, &self.lockfile, &self.rustc]
                .iter()
                .all(|field| field.as_str() != UNKNOWN)
    }
}

/// One assumption a scenario declares, with its parameters (properties.md,
/// Assumptions). Empty until the property registry lands.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AssumeRecord {
    /// The assumption, such as `bounded-delivery`.
    pub assume: String,
    /// Its parameters, such as the link's maximum delay. Written with sorted
    /// keys (see [`sorted`]).
    #[serde(serialize_with = "sorted")]
    pub params: Value,
}

/// The scheduler limits a run used.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchedulerRecord {
    /// Simulated runtime workers.
    pub workers: u16,
    /// Stack size for each task, in bytes.
    pub stack_size: usize,
    /// The most tasks the run may spawn.
    pub max_tasks: u32,
}

impl From<SchedulerConfig> for SchedulerRecord {
    /// The record of `config`'s limits. The UTC origin isn't a limit, and the
    /// scenario holds it.
    fn from(config: SchedulerConfig) -> Self {
        SchedulerRecord {
            workers: config.workers,
            stack_size: config.stack_size,
            max_tasks: config.max_tasks,
        }
    }
}

/// The step limits a run used.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BudgetRecord {
    /// Live steps before the drain starts.
    pub budget: u64,
    /// The most steps the drain takes.
    pub drain_bound: u64,
    /// The most steps at one instant before a run counts as a livelock.
    pub max_steps_per_instant: u64,
    /// Whether passing `max_steps_per_instant` violates `sim/no-livelock`.
    pub enforce_no_livelock: bool,
}

impl From<&RunConfig> for BudgetRecord {
    /// The record of `config`'s step limits.
    fn from(config: &RunConfig) -> Self {
        BudgetRecord {
            budget: config.budget,
            drain_bound: config.drain_bound,
            max_steps_per_instant: config.max_steps_per_instant,
            enforce_no_livelock: config.enforce_no_livelock,
        }
    }
}

/// A blob a trace needs, by name and digest.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BlobRef {
    /// What the scenario calls it, such as `chain`.
    pub name: String,
    /// The blob's SHA-256 in lowercase hex, which is also its file name.
    pub sha256: String,
}

/// A property the run checks.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PropertyRef {
    /// The property's ID.
    pub id: String,
    /// Its status when the trace was written.
    pub status: PropertyStatus,
}

/// A property's status (properties.md, Statuses).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub enum PropertyStatus {
    /// An enforced claim.
    Active,
    /// An open bug violates it.
    Known {
        /// The bug's issue number.
        issue: u32,
    },
    /// The claim itself is under test.
    Draft,
}

/// The `input.bin` bytes that produced a run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputRef {
    /// The bytes' SHA-256 in lowercase hex.
    pub sha256: String,
    /// Their length.
    pub len: u64,
}

/// Where a derived trace came from.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Derivation {
    /// The source trace's ID, the SHA-256 of its `trace.jsonl`.
    pub trace: String,
    /// The tool that derived it.
    pub tool: DerivationTool,
    /// The tool's version.
    pub tool_version: u32,
}

/// A tool that derives one trace from another.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DerivationTool {
    /// A saved Recover replay.
    Recover,
    /// The shrinker.
    Shrink,
    /// Schedule search.
    Search,
    /// A format migration.
    Migrate,
}

/// Serializes `value` with every object's keys in sorted order, at any depth.
///
/// Equal records must write equal bytes. serde_json keeps an object's
/// insertion order when its `preserve_order` feature is on, which another
/// workspace crate turns on, so `{"a":1,"z":2}` and `{"z":2,"a":1}` would
/// compare equal but write different bytes and trace IDs.
fn sorted<S: Serializer>(value: &Value, serializer: S) -> Result<S::Ok, S::Error> {
    Sorted(value).serialize(serializer)
}

/// A JSON value that serializes with sorted object keys (see [`sorted`]).
struct Sorted<'a>(&'a Value);

impl Serialize for Sorted<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.0 {
            Value::Object(map) => {
                let mut entries: Vec<(&String, &Value)> = map.iter().collect();
                entries.sort_unstable_by(|a, b| a.0.cmp(b.0));
                let mut out = serializer.serialize_map(Some(entries.len()))?;
                for (key, value) in entries {
                    out.serialize_entry(key, &Sorted(value))?;
                }
                out.end()
            }
            Value::Array(items) => serializer.collect_seq(items.iter().map(Sorted)),
            other => other.serialize(serializer),
        }
    }
}

/// Splits a list `build.rs` joined with its separator.
fn list(joined: &str) -> Vec<String> {
    joined
        .split('\u{1f}')
        .filter(|entry| !entry.is_empty())
        .map(str::to_owned)
        .collect()
}

/// `bytes` as lowercase hex.
pub(crate) fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
