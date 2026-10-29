//! Curated fuzz seeds: committed inputs, their sidecars, and their
//! validation against the current generators (explorer implementation
//! plan, E3a-1).
//!
//! Each seed is a `<name>.bin` file with a `<name>.toml` sidecar beside it,
//! in a fixture directory such as `crates/zakura-sim/tests/fixtures/`. The
//! sidecar records the harness that runs the seed, the generator version it
//! was built for, the file's SHA-256, the scenario it decodes to, where its
//! expectation comes from, and the property witnesses it carries.
//!
//! A seed is one of two kinds:
//!
//! - An `input` seed is a full fuzz input: the harness's scenario prefix,
//!   then the Plan bytes.
//! - A `plan` seed is Plan bytes alone. Its sidecar gives the scenario
//!   prefix that encodes its scenario, such as a saved Plan's tag, and
//!   [`load`] joins the two into a full fuzz input.
//!
//! [`load`] admits a seed only if its harness is registered at the
//! sidecar's generator, its bytes match the digest, its prefix decodes to
//! the recorded scenario, that scenario round-trips through the harness's
//! scenario type, and each witness names a property the harness registers.
//! A required seed that fails is rejected. An optional one is quarantined.
//! Either way the report gives the reason.

use std::{
    fmt, fs, io,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{
    harness::{self, HarnessEntry},
    record::{CONTRACT_VERSION, FORMAT_VERSION},
    trace::meta::hex,
    witness::Expect,
};

/// The extension of a seed's bytes.
pub const SEED_EXTENSION: &str = "bin";

/// The extension of a seed's sidecar.
pub const SIDECAR_EXTENSION: &str = "toml";

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

/// What a seed's bytes hold.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SeedKind {
    /// A full fuzz input: the scenario prefix, then the Plan bytes.
    #[default]
    Input,
    /// Plan bytes alone. The sidecar's `prefix` encodes the scenario.
    Plan,
}

/// A property witness a seed carries: the witness test of `property` on the
/// seed expects `expect`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SeedWitness {
    /// The property's ID. The harness must register it.
    pub property: String,
    /// `holds`, `violated` or `applicable`, as `zakura-sim record --expect`
    /// takes it.
    pub expect: String,
}

/// A seed's sidecar, `<name>.toml` beside `<name>.bin`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Sidecar {
    /// The harness that runs the seed.
    pub harness: String,
    /// The harness's generator version when the seed was built.
    pub generator: u32,
    /// The SHA-256 of the seed's file, in hex.
    pub sha256: String,
    /// What the file holds. `input` by default.
    #[serde(default)]
    pub kind: SeedKind,
    /// For a `plan` seed, the scenario prefix that encodes its scenario, in
    /// hex. An `input` seed carries its prefix in its bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    /// The SHA-256 of the scenario the seed decodes to, from
    /// [`scenario_sha256`].
    pub scenario_sha256: String,
    /// Where the seed's expectation comes from: the regression test or plan
    /// that runs it.
    pub source: String,
    /// Whether the seed must validate. A required seed that fails rejects
    /// the seed set; an optional one is quarantined. `true` by default.
    #[serde(default = "required_by_default")]
    pub required: bool,
    /// The property witnesses the seed carries. None by default.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub witness: Vec<SeedWitness>,
}

/// [`Sidecar::required`]'s default.
fn required_by_default() -> bool {
    true
}

/// Why a seed failed validation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SeedError {
    /// The `.bin` file has no sidecar.
    NoSidecar,
    /// The sidecar has no `.bin` file beside it.
    NoInput,
    /// A file couldn't be read.
    Read(String),
    /// The sidecar isn't a valid sidecar.
    Sidecar(String),
    /// The sidecar names no registered harness.
    UnknownHarness(String),
    /// The seed was built for another generator version.
    Generator {
        /// The sidecar's generator.
        seed: u32,
        /// The harness's current generator.
        harness: u32,
    },
    /// The file's SHA-256 isn't the sidecar's.
    Digest {
        /// The sidecar's digest.
        recorded: String,
        /// The file's digest.
        actual: String,
    },
    /// An `input` seed's sidecar gives a `prefix`.
    PrefixOnInput,
    /// A `plan` seed's sidecar gives no `prefix`. No harness registers a
    /// scenario encoder yet, so a `plan` seed must name its prefix.
    NoPrefix,
    /// A `plan` seed's `prefix` isn't hex of the harness's prefix length.
    Prefix {
        /// The sidecar's prefix.
        prefix: String,
        /// The harness's prefix length in bytes.
        scenario_bytes: usize,
    },
    /// The decoded scenario doesn't round-trip through the harness's
    /// scenario type.
    RoundTrip(String),
    /// The seed decodes to another scenario than the sidecar records.
    Scenario {
        /// The sidecar's scenario digest.
        recorded: String,
        /// The decoded scenario's digest.
        decoded: String,
    },
    /// The sidecar's `source` is empty.
    NoSource,
    /// A witness names a property the harness doesn't register.
    Unregistered(String),
    /// A witness's `expect` isn't an expectation.
    Expect {
        /// The witness's property.
        property: String,
        /// Why `expect` doesn't parse.
        error: String,
    },
    /// Two witnesses name the same property.
    DuplicateWitness(String),
}

impl fmt::Display for SeedError {
    /// Says what failed, with the values that disagree.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SeedError::NoSidecar => write!(f, "the seed has no .{SIDECAR_EXTENSION} sidecar"),
            SeedError::NoInput => write!(f, "the sidecar has no .{SEED_EXTENSION} seed beside it"),
            SeedError::Read(error) => write!(f, "reading the seed: {error}"),
            SeedError::Sidecar(error) => write!(f, "the sidecar doesn't parse: {error}"),
            SeedError::UnknownHarness(name) => write!(f, "no harness is named `{name}`"),
            SeedError::Generator { seed, harness } => write!(
                f,
                "the seed was built for generator {seed}, and the harness is at generator \
                 {harness}"
            ),
            SeedError::Digest { recorded, actual } => write!(
                f,
                "the sidecar records sha256 {recorded}, and the file's is {actual}"
            ),
            SeedError::PrefixOnInput => write!(
                f,
                "an `input` seed carries its prefix in its bytes; `prefix` is for `plan` seeds"
            ),
            SeedError::NoPrefix => write!(
                f,
                "a `plan` seed needs `prefix`, the scenario prefix that encodes its scenario"
            ),
            SeedError::Prefix {
                prefix,
                scenario_bytes,
            } => write!(
                f,
                "the prefix `{prefix}` isn't {scenario_bytes} bytes of hex, the harness's \
                 scenario prefix length"
            ),
            SeedError::RoundTrip(error) => write!(
                f,
                "the decoded scenario doesn't round-trip through the harness's scenario type: \
                 {error}"
            ),
            SeedError::Scenario { recorded, decoded } => write!(
                f,
                "the seed decodes to scenario sha256 {decoded}, and the sidecar records \
                 {recorded}"
            ),
            SeedError::NoSource => write!(f, "the sidecar's `source` is empty"),
            SeedError::Unregistered(property) => {
                write!(
                    f,
                    "the harness doesn't register the witness's property `{property}`"
                )
            }
            SeedError::Expect { property, error } => {
                write!(f, "the witness of `{property}`: {error}")
            }
            SeedError::DuplicateWitness(property) => {
                write!(f, "two witnesses name `{property}`")
            }
        }
    }
}

impl std::error::Error for SeedError {}

/// A seed that passed validation.
#[derive(Clone, Debug)]
pub struct Seed {
    /// The seed's `.bin` file.
    pub path: PathBuf,
    /// Its sidecar.
    pub sidecar: Sidecar,
    /// The full fuzz input: the file for an `input` seed, the prefix then
    /// the file for a `plan` seed.
    pub input: Vec<u8>,
}

impl Seed {
    /// The SHA-256 of [`Seed::input`], in hex, which names it in a corpus.
    pub fn input_sha256(&self) -> String {
        sha256(&self.input)
    }
}

/// A seed that failed validation.
#[derive(Clone, Debug)]
pub struct Failed {
    /// The seed's `.bin` file, or its sidecar if it has no `.bin` file.
    pub path: PathBuf,
    /// The sidecar, if it parsed.
    pub sidecar: Option<Sidecar>,
    /// Why it failed.
    pub error: SeedError,
}

/// What [`load`] found.
#[derive(Debug, Default)]
pub struct SeedReport {
    /// The seeds that passed.
    pub admitted: Vec<Seed>,
    /// The optional seeds that failed.
    pub quarantined: Vec<Failed>,
    /// The required seeds that failed, and the files whose sidecar can't be
    /// read.
    pub rejected: Vec<Failed>,
    /// How many valid sidecars name another harness than the one asked for.
    pub other_harnesses: usize,
}

/// Reads and validates every seed in `dirs`. With `harness`, it validates
/// only that harness's seeds and counts the rest in
/// [`SeedReport::other_harnesses`]. A seed whose sidecar doesn't parse is
/// rejected whatever `harness` is, since nothing says which harness or
/// requirement it has. A seed whose sidecar names no registered harness is
/// rejected or quarantined by its requirement whatever `harness` is, since
/// a misspelled name could be `harness`.
///
/// Fails only if a directory can't be listed.
pub fn load(dirs: &[PathBuf], harness: Option<&str>) -> io::Result<SeedReport> {
    let mut report = SeedReport::default();
    for dir in dirs {
        let mut paths: Vec<PathBuf> = fs::read_dir(dir)?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect::<io::Result<_>>()?;
        paths.sort();
        for path in paths {
            if !path.is_file() {
                continue;
            }
            let extension = path.extension().and_then(|e| e.to_str());
            if extension == Some(SIDECAR_EXTENSION) {
                if !path.with_extension(SEED_EXTENSION).is_file() {
                    report.rejected.push(Failed {
                        path,
                        sidecar: None,
                        error: SeedError::NoInput,
                    });
                }
                continue;
            }
            if extension != Some(SEED_EXTENSION) {
                continue;
            }
            let sidecar = match read_sidecar(&path) {
                Ok(sidecar) => sidecar,
                Err(error) => {
                    report.rejected.push(Failed {
                        path,
                        sidecar: None,
                        error,
                    });
                    continue;
                }
            };
            let entry = match harness::find(&sidecar.harness) {
                Ok(entry) => entry,
                Err(_) => {
                    let error = SeedError::UnknownHarness(sidecar.harness.clone());
                    report.fail(path, sidecar, error);
                    continue;
                }
            };
            if harness.is_some_and(|name| name != entry.name) {
                report.other_harnesses += 1;
                continue;
            }
            match validate(entry, &path, &sidecar) {
                Ok(input) => report.admitted.push(Seed {
                    path,
                    sidecar,
                    input,
                }),
                Err(error) => report.fail(path, sidecar, error),
            }
        }
    }
    Ok(report)
}

impl SeedReport {
    /// Records that the seed at `path` failed with `error`: rejected if
    /// `sidecar` requires it, quarantined if not.
    fn fail(&mut self, path: PathBuf, sidecar: Sidecar, error: SeedError) {
        let required = sidecar.required;
        let failed = Failed {
            path,
            sidecar: Some(sidecar),
            error,
        };
        if required {
            self.rejected.push(failed);
        } else {
            self.quarantined.push(failed);
        }
    }
}

/// The sidecar beside the seed at `path`.
fn read_sidecar(path: &Path) -> Result<Sidecar, SeedError> {
    let sidecar = path.with_extension(SIDECAR_EXTENSION);
    let text = match fs::read_to_string(&sidecar) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Err(SeedError::NoSidecar),
        Err(error) => return Err(SeedError::Read(format!("{}: {error}", sidecar.display()))),
    };
    toml::from_str(&text).map_err(|error| SeedError::Sidecar(error.to_string()))
}

/// Checks the seed at `path` against `sidecar` and `entry`, and returns its
/// full fuzz input.
fn validate(entry: &HarnessEntry, path: &Path, sidecar: &Sidecar) -> Result<Vec<u8>, SeedError> {
    if sidecar.generator != entry.generator {
        return Err(SeedError::Generator {
            seed: sidecar.generator,
            harness: entry.generator,
        });
    }
    let bytes = fs::read(path).map_err(|error| SeedError::Read(error.to_string()))?;
    let actual = sha256(&bytes);
    if !sidecar.sha256.eq_ignore_ascii_case(&actual) {
        return Err(SeedError::Digest {
            recorded: sidecar.sha256.clone(),
            actual,
        });
    }
    let input = match (sidecar.kind, &sidecar.prefix) {
        (SeedKind::Input, None) => bytes,
        (SeedKind::Input, Some(_)) => return Err(SeedError::PrefixOnInput),
        (SeedKind::Plan, None) => return Err(SeedError::NoPrefix),
        (SeedKind::Plan, Some(prefix)) => {
            let mut input = unhex(prefix)
                .filter(|decoded| decoded.len() == entry.scenario_bytes)
                .ok_or_else(|| SeedError::Prefix {
                    prefix: prefix.clone(),
                    scenario_bytes: entry.scenario_bytes,
                })?;
            input.extend_from_slice(&bytes);
            input
        }
    };
    let decoded = decode(entry, &input)?;
    if !sidecar.scenario_sha256.eq_ignore_ascii_case(&decoded) {
        return Err(SeedError::Scenario {
            recorded: sidecar.scenario_sha256.clone(),
            decoded,
        });
    }
    if sidecar.source.trim().is_empty() {
        return Err(SeedError::NoSource);
    }
    let mut seen = Vec::new();
    for witness in &sidecar.witness {
        if entry.status(&witness.property).is_none() {
            return Err(SeedError::Unregistered(witness.property.clone()));
        }
        if let Err(error) = witness.expect.parse::<Expect>() {
            return Err(SeedError::Expect {
                property: witness.property.clone(),
                error,
            });
        }
        if seen.contains(&&witness.property) {
            return Err(SeedError::DuplicateWitness(witness.property.clone()));
        }
        seen.push(&witness.property);
    }
    Ok(input)
}

/// The digest of the scenario the full fuzz `input` decodes to on `entry`,
/// as the explorer decodes it: the prefix, zero-padded to the harness's
/// length. Checks that the scenario round-trips through the harness's
/// scenario type.
pub fn decode(entry: &HarnessEntry, input: &[u8]) -> Result<String, SeedError> {
    let mut prefix = vec![0u8; entry.scenario_bytes];
    let n = input.len().min(entry.scenario_bytes);
    prefix[..n].copy_from_slice(&input[..n]);
    let drawn = (entry.draw)(&prefix);
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

/// `bytes`' SHA-256 in lowercase hex, as sidecars record it.
pub fn sha256(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}

/// `text` as bytes, if it's an even number of hex digits.
fn unhex(text: &str) -> Option<Vec<u8>> {
    // `from_str_radix` also takes a leading `+`.
    if !text.len().is_multiple_of(2) || !text.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }
    (0..text.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(text.get(index..index + 2)?, 16).ok())
        .collect()
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

    /// Hex decodes only in whole bytes.
    #[test]
    fn unhex_takes_whole_bytes() {
        assert_eq!(unhex("ff06"), Some(vec![0xff, 0x06]));
        assert_eq!(unhex(""), Some(Vec::new()));
        assert_eq!(unhex("f"), None);
        assert_eq!(unhex("zz"), None);
        assert_eq!(unhex("ff+6"), None);
    }
}
