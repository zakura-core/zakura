//! Curated fuzz seeds, built in code (explorer implementation plan, E3a-1).
//!
//! Each seed is a full fuzz input (the harness's scenario prefix, then the
//! Plan bytes) that a function in this module builds from typed fields,
//! a saved scenario's tag, or a fixed draw from [`draws::byte_inputs`]. No
//! seed is committed as bytes: `zakura-sim seeds --out` writes the corpus
//! when a fuzz campaign needs it, and `zakura-sim record --seed` runs one
//! seed by name.
//!
//! A seed belongs to one [`SeedSet`]. The [corpus](SeedSet::Corpus) is the
//! set fuzz campaigns, target checks and `zakura-sim seeds --out` start
//! from, so each of its seeds runs as an input that passes. A seed that
//! fails by design sits in [`SeedSet::KnownFailure`], and one that runs too
//! long for a fuzz input sits in [`SeedSet::Slow`]. Only its own test runs
//! such a seed.
//!
//! [`load`] admits a seed only if its harness is registered, the harness
//! accepts its input, the decoded scenario round-trips through the
//! harness's scenario type, it names the test that runs it, each witness
//! names a property the harness registers, and its witnesses fit its set.
//! A seed that fails is rejected, and the report gives the reason.

mod block;
mod discovery;
pub mod draws;
mod handshake;
mod header;
mod race;
mod sync;

use std::{
    collections::BTreeSet,
    fmt,
    path::{Path, PathBuf},
};

use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{
    harness::{self, HarnessEntry},
    record::{CONTRACT_VERSION, FORMAT_VERSION},
    trace::meta::hex,
    witness::Expect,
};

/// The versions that fix what `entry`'s fuzz input means, as `KEY=VALUE`
/// pairs in a stable order: the harness, its generator, its scenario prefix
/// length, and the trace format and contract versions. A campaign keys its
/// corpus on all of them, so a new version added here changes the key.
pub fn versions(entry: &HarnessEntry) -> Vec<(&'static str, String)> {
    vec![
        ("harness", entry.name.to_owned()),
        ("generator", entry.generator.to_string()),
        ("scenario_bytes", entry.scenario_bytes.to_string()),
        ("format", FORMAT_VERSION.to_string()),
        ("contract", CONTRACT_VERSION.to_string()),
    ]
}

/// Which runs use a seed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum SeedSet {
    /// The seed corpus: fuzz campaigns and target checks start from it, so
    /// each seed passes.
    #[default]
    Corpus,
    /// Seeds that fail by design. Each carries a witness that expects a
    /// violation, which its own test checks.
    KnownFailure,
    /// Seeds that pass but run too long for a fuzz input.
    Slow,
}

impl SeedSet {
    /// The set `name` names, as `zakura-sim seeds --set` takes it.
    pub fn parse(name: &str) -> Option<SeedSet> {
        match name {
            "corpus" => Some(SeedSet::Corpus),
            "known-failure" => Some(SeedSet::KnownFailure),
            "slow" => Some(SeedSet::Slow),
            _ => None,
        }
    }
}

/// A property witness a seed carries: the witness test of `property` on the
/// seed expects `expect`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct SeedWitness {
    /// The property's ID. The harness must register it.
    pub property: String,
    /// [`Expect::name`]: `holds`, `violated` or `applicable`, as
    /// `zakura-sim record --expect` takes it.
    pub expect: String,
}

/// One seed: its name, the harness that runs it, its set, the test that
/// runs it, its witnesses and its full fuzz input.
#[derive(Clone, Debug, Serialize)]
pub struct Seed {
    /// The seed's name, unique across every set.
    pub name: String,
    /// The harness that runs the seed.
    pub harness: String,
    /// The runs that use the seed.
    pub set: SeedSet,
    /// The test that runs the seed, as a path from the repository's root.
    pub source: String,
    /// The property witnesses the seed carries.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub witness: Vec<SeedWitness>,
    /// The full fuzz input: the scenario prefix, then the Plan bytes.
    #[serde(skip)]
    pub input: Vec<u8>,
}

impl Seed {
    /// A corpus seed named `name` for `harness` with no witnesses, run by
    /// the test at `source`.
    pub fn new(name: impl Into<String>, harness: &str, source: &str, input: Vec<u8>) -> Seed {
        Seed {
            name: name.into(),
            harness: harness.to_owned(),
            set: SeedSet::Corpus,
            source: source.to_owned(),
            witness: Vec::new(),
            input,
        }
    }

    /// The seed in `set`.
    pub fn in_set(self, set: SeedSet) -> Seed {
        Seed { set, ..self }
    }

    /// The seed with one more witness: `property` expects `expect`.
    pub fn witness(mut self, property: &str, expect: Expect) -> Seed {
        self.witness.push(SeedWitness {
            property: property.to_owned(),
            expect: expect.name().to_owned(),
        });
        self
    }

    /// The SHA-256 of [`Seed::input`], in hex, which names it in a corpus.
    pub fn input_sha256(&self) -> String {
        sha256(&self.input)
    }

    /// Whether a witness expects its property violated, so the seed fails by
    /// design.
    pub fn expects_violation(&self) -> bool {
        self.witness
            .iter()
            .any(|witness| matches!(witness.expect.parse(), Ok(Expect::Violated)))
    }
}

/// Every seed, in every set, sorted by name.
pub fn all() -> Vec<Seed> {
    let mut seeds: Vec<Seed> = [
        block::seeds(),
        discovery::seeds(),
        handshake::seeds(),
        header::seeds(),
        race::seeds(),
        sync::seeds(),
    ]
    .into_iter()
    .flatten()
    .collect();
    seeds.sort_by(|a, b| a.name.cmp(&b.name));
    seeds
}

/// The seed named `name`, in any set.
pub fn find(name: &str) -> Option<Seed> {
    all().into_iter().find(|seed| seed.name == name)
}

/// Why a seed failed validation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SeedError {
    /// Another seed has the same name.
    DuplicateName,
    /// The seed names no registered harness.
    UnknownHarness(String),
    /// The seed's prefix is for another generator than the harness's, so the
    /// harness would draw a fallback scenario for it.
    OtherGenerator,
    /// The decoded scenario doesn't round-trip through the harness's
    /// scenario type.
    RoundTrip(String),
    /// The seed's `source` is empty.
    NoSource,
    /// A witness names a property the harness doesn't register.
    Unregistered(String),
    /// Two witnesses name the same property.
    DuplicateWitness(String),
    /// The seed's witnesses don't fit its set: a corpus or slow seed expects
    /// a violation, or a known failure expects none.
    WrongSet(SeedSet),
}

impl fmt::Display for SeedError {
    /// Says what failed, with the values that disagree.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SeedError::DuplicateName => write!(f, "another seed has the same name"),
            SeedError::UnknownHarness(name) => write!(f, "no harness is named `{name}`"),
            SeedError::OtherGenerator => write!(
                f,
                "the seed's prefix is for another generator, so the harness draws its fallback \
                 scenario"
            ),
            SeedError::RoundTrip(error) => write!(
                f,
                "the decoded scenario doesn't round-trip through the harness's scenario type: \
                 {error}"
            ),
            SeedError::NoSource => write!(f, "the seed's `source` is empty"),
            SeedError::Unregistered(property) => write!(
                f,
                "the harness doesn't register the witness's property `{property}`"
            ),
            SeedError::DuplicateWitness(property) => {
                write!(f, "two witnesses name `{property}`")
            }
            SeedError::WrongSet(SeedSet::KnownFailure) => write!(
                f,
                "a known failure needs a witness that expects a violation"
            ),
            SeedError::WrongSet(set) => write!(
                f,
                "a witness expects a violation, so the seed belongs in the known failures, not \
                 {set:?}"
            ),
        }
    }
}

impl std::error::Error for SeedError {}

/// A seed that failed validation.
#[derive(Clone, Debug)]
pub struct Failed {
    /// The seed.
    pub seed: Seed,
    /// Why it failed.
    pub error: SeedError,
}

/// What [`load`] found.
#[derive(Debug, Default)]
pub struct SeedReport {
    /// The seeds that passed.
    pub admitted: Vec<Seed>,
    /// The seeds that failed.
    pub rejected: Vec<Failed>,
    /// How many seeds in the set are for another harness than the one asked
    /// for.
    pub other_harnesses: usize,
}

/// Builds and validates every seed in `set`. With `harness`, it validates
/// only that harness's seeds and counts the rest in
/// [`SeedReport::other_harnesses`]. A seed that names no registered harness
/// is rejected whatever `harness` is, since a misspelled name could be
/// `harness`.
pub fn load(set: SeedSet, harness: Option<&str>) -> SeedReport {
    let mut report = SeedReport::default();
    let seeds = all();
    let mut names = BTreeSet::new();
    let duplicates: BTreeSet<&str> = seeds
        .iter()
        .filter(|seed| !names.insert(seed.name.as_str()))
        .map(|seed| seed.name.as_str())
        .collect();
    let duplicates: BTreeSet<String> = duplicates.into_iter().map(str::to_owned).collect();
    for seed in seeds.into_iter().filter(|seed| seed.set == set) {
        let entry = match harness::find(&seed.harness) {
            Ok(entry) => entry,
            Err(_) => {
                let error = SeedError::UnknownHarness(seed.harness.clone());
                report.rejected.push(Failed { seed, error });
                continue;
            }
        };
        if harness.is_some_and(|name| name != entry.name) {
            report.other_harnesses += 1;
            continue;
        }
        let checked = if duplicates.contains(&seed.name) {
            Err(SeedError::DuplicateName)
        } else {
            validate(entry, &seed)
        };
        match checked {
            Ok(()) => report.admitted.push(seed),
            Err(error) => report.rejected.push(Failed { seed, error }),
        }
    }
    report
}

/// [`SeedReport`] as `zakura-sim seeds --report` writes it, and as the
/// suite manifest embeds it: one row per seed.
#[derive(Clone, Debug, Serialize)]
pub struct Report {
    /// The harness the report is limited to, if any.
    pub harness: Option<String>,
    /// The set the report covers.
    pub set: SeedSet,
    /// Every seed, admitted ones first.
    pub seeds: Vec<ReportRow>,
    /// [`SeedReport::other_harnesses`].
    pub other_harnesses: usize,
}

/// One seed in a [`Report`]: its status, the seed, and either its full
/// input's digest or its reason.
#[derive(Clone, Debug, Serialize)]
pub struct ReportRow {
    /// `admitted` or `rejected`.
    pub status: &'static str,
    /// The seed.
    #[serde(flatten)]
    pub seed: Seed,
    /// The full fuzz input's SHA-256, for an admitted seed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_sha256: Option<String>,
    /// Why the seed failed, for a seed that did.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl SeedReport {
    /// The report's rows, for `harness` and `set`.
    pub fn rows(&self, harness: Option<&str>, set: SeedSet) -> Report {
        let admitted = self.admitted.iter().map(|seed| ReportRow {
            status: "admitted",
            seed: seed.clone(),
            input_sha256: Some(seed.input_sha256()),
            reason: None,
        });
        let rejected = self.rejected.iter().map(|failed| ReportRow {
            status: "rejected",
            seed: failed.seed.clone(),
            input_sha256: None,
            reason: Some(failed.error.to_string()),
        });
        Report {
            harness: harness.map(str::to_owned),
            set,
            seeds: admitted.chain(rejected).collect(),
            other_harnesses: self.other_harnesses,
        }
    }
}

/// The repository's root: two levels above this crate.
pub fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// `path` relative to the repository's root if it's in the repository,
/// and as given if not.
pub fn from_root(path: &Path) -> PathBuf {
    path.canonicalize()
        .ok()
        .zip(repo_root().canonicalize().ok())
        .and_then(|(path, root)| path.strip_prefix(root).ok().map(Path::to_owned))
        .unwrap_or_else(|| path.to_owned())
}

/// Checks `seed` against `entry`.
fn validate(entry: &HarnessEntry, seed: &Seed) -> Result<(), SeedError> {
    if !entry.accepts_input(&seed.input) {
        return Err(SeedError::OtherGenerator);
    }
    decode(entry, &seed.input)?;
    if seed.source.trim().is_empty() {
        return Err(SeedError::NoSource);
    }
    let mut seen = Vec::new();
    for witness in &seed.witness {
        if entry.status(&witness.property).is_none() {
            return Err(SeedError::Unregistered(witness.property.clone()));
        }
        if seen.contains(&&witness.property) {
            return Err(SeedError::DuplicateWitness(witness.property.clone()));
        }
        seen.push(&witness.property);
    }
    let fits = match seed.set {
        SeedSet::KnownFailure => seed.expects_violation(),
        SeedSet::Corpus | SeedSet::Slow => !seed.expects_violation(),
    };
    if !fits {
        return Err(SeedError::WrongSet(seed.set));
    }
    Ok(())
}

/// The digest of the scenario the full fuzz `input` decodes to on `entry`,
/// using the harness's prefix and padding rule. Checks that the scenario
/// round-trips through the harness's scenario type.
pub fn decode(entry: &HarnessEntry, input: &[u8]) -> Result<String, SeedError> {
    let drawn = entry.scenario(input);
    let meta = (entry.meta)(&drawn).map_err(|error| SeedError::RoundTrip(error.to_string()))?;
    let digest = scenario_sha256(&drawn);
    let reserialized = scenario_sha256(&meta.scenario);
    if reserialized != digest {
        return Err(SeedError::RoundTrip(format!(
            "it reserializes to scenario sha256 {reserialized}, not {digest}"
        )));
    }
    Ok(digest)
}

/// The SHA-256 of `scenario`'s canonical JSON, in hex: object keys sorted,
/// no whitespace. It doesn't depend on whether `serde_json` keeps
/// insertion order, which feature unification decides per build.
pub fn scenario_sha256(scenario: &Value) -> String {
    let mut text = String::new();
    canonical(scenario, &mut text);
    sha256(text.as_bytes())
}

/// Appends `value`'s canonical JSON to `out`.
fn canonical(value: &Value, out: &mut String) {
    match value {
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            out.push('{');
            for (index, key) in keys.into_iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                out.push_str(&Value::String(key.clone()).to_string());
                out.push(':');
                canonical(&map[key], out);
            }
            out.push('}');
        }
        Value::Array(items) => {
            out.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                canonical(item, out);
            }
            out.push(']');
        }
        scalar => out.push_str(&scalar.to_string()),
    }
}

/// `bytes`' SHA-256 in lowercase hex.
pub fn sha256(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    /// Key order doesn't change the digest, and a value does.
    #[test]
    fn scenario_digest_ignores_key_order() {
        let a = json!({"b": [1, {"y": true, "x": "s"}], "a": null});
        let b = json!({"a": null, "b": [1, {"x": "s", "y": true}]});
        assert_eq!(scenario_sha256(&a), scenario_sha256(&b));
        assert_ne!(
            scenario_sha256(&a),
            scenario_sha256(&json!({"a": null, "b": [1, {"x": "t", "y": true}]}))
        );
    }
}
