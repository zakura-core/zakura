//! The simulator's seed registry, as a built `zakura-sim` lists it.
//!
//! A planted case or a mutation-loop witness names a seed by name. The
//! registry, `zakura_sim::seed`, builds each seed's input in code and names
//! its harness. xtask can't link `zakura-sim`, which builds only with the
//! simulator's flag, so [`Registry::read`] runs a built `zakura-sim seeds
//! --report` once per set and reads the rows.

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use serde::Deserialize;
use sha2::{Digest, Sha256};

use super::{cargo_sim, pilot::hex, PROFILE};
use crate::{run_command, BoxError};

/// The registry's source, relative to the repository root: the files a
/// seed's input comes from.
pub(super) const REGISTRY: &str = "crates/zakura-sim/src/seed";

/// Every seed set, as `zakura-sim seeds --set` takes it.
const SETS: [&str; 3] = ["corpus", "known-failure", "slow"];

/// One seed, as `zakura-sim seeds --report` lists it.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub(super) struct SeedRow {
    /// The seed's name, unique across every set.
    pub(super) name: String,
    /// The harness that runs it.
    pub(super) harness: String,
    /// Its set: `corpus`, `known-failure` or `slow`.
    pub(super) set: String,
    /// `admitted` or `rejected`.
    pub(super) status: String,
    /// The full fuzz input's SHA-256, for an admitted seed.
    #[serde(default)]
    pub(super) input_sha256: Option<String>,
    /// Why validation rejected the seed, for a rejected one.
    #[serde(default)]
    pub(super) reason: Option<String>,
}

/// The parts of `zakura-sim seeds --report`'s JSON that [`Registry::read`]
/// reads.
#[derive(Deserialize)]
struct ReportFile {
    seeds: Vec<SeedRow>,
}

/// Every seed in every set, by name.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct Registry {
    seeds: BTreeMap<String, SeedRow>,
}

impl Registry {
    /// Lists every set with `zakura_sim`, a built `zakura-sim`, writing each
    /// set's report under `dir`.
    pub(super) fn read(zakura_sim: &Path, dir: &Path) -> Result<Registry, BoxError> {
        fs::create_dir_all(dir)?;
        let mut rows = Vec::new();
        for set in SETS {
            let report = dir.join(format!("{set}.json"));
            if report.exists() {
                fs::remove_file(&report)?;
            }
            let output = Command::new(zakura_sim)
                .args(["seeds", "--set", set, "--report"])
                .arg(&report)
                .output()?;
            // `seeds` exits 1 when it rejects a seed, after writing the
            // report, which lists the rejected seed with its reason.
            let text = fs::read_to_string(&report).map_err(|_| {
                format!(
                    "`zakura-sim seeds --set {set}` wrote no report: {}: {}",
                    output.status,
                    String::from_utf8_lossy(&output.stderr).trim()
                )
            })?;
            let file: ReportFile = serde_json::from_str(&text)
                .map_err(|error| format!("{}: {error}", report.display()))?;
            rows.extend(file.seeds);
        }
        Registry::from_rows(rows)
    }

    /// The registry of `rows`. Fails on a name listed twice.
    pub(super) fn from_rows(rows: impl IntoIterator<Item = SeedRow>) -> Result<Registry, BoxError> {
        let mut seeds = BTreeMap::new();
        for row in rows {
            if let Some(earlier) = seeds.insert(row.name.clone(), row) {
                return Err(format!("the registry lists the seed {} twice", earlier.name).into());
            }
        }
        Ok(Registry { seeds })
    }

    /// The admitted seed named `name`. Fails on a name the registry lacks
    /// and on a seed that validation rejected.
    pub(super) fn seed(&self, name: &str) -> Result<&SeedRow, BoxError> {
        let seed = self
            .seeds
            .get(name)
            .ok_or_else(|| format!("no seed is named `{name}` in {REGISTRY}"))?;
        if seed.status != "admitted" {
            return Err(format!(
                "the seed {name} fails validation: {}",
                seed.reason.as_deref().unwrap_or("no reason given")
            )
            .into());
        }
        Ok(seed)
    }

    /// The harness of the admitted seed named `name` ([`Registry::seed`]).
    pub(super) fn harness(&self, name: &str) -> Result<String, BoxError> {
        Ok(self.seed(name)?.harness.clone())
    }

    /// Every admitted seed's input digest.
    pub(super) fn input_digests(&self) -> impl Iterator<Item = &str> {
        self.seeds
            .values()
            .filter_map(|seed| seed.input_sha256.as_deref())
    }
}

/// Builds `checkout`'s `zakura-sim` in `target` ([`cargo_sim`]) and lists
/// its registry, writing the reports under `target`.
pub(super) fn build(checkout: &Path, target: &Path) -> Result<Registry, BoxError> {
    run_command(&mut cargo_sim(checkout, target))?;
    Registry::read(
        &target.join(PROFILE).join("zakura-sim"),
        &target.join("seeds"),
    )
}

/// Writes `seed`'s full fuzz input with `zakura_sim`, a built `zakura-sim`,
/// into `dir`, and returns its file. `zakura-sim seeds --out` writes every
/// admitted seed of the seed's harness and set, each named by its SHA-256,
/// so the file is `dir/<input_sha256>`. Fails unless the file holds the
/// input the registry lists.
pub(super) fn write_input(
    zakura_sim: &Path,
    seed: &SeedRow,
    dir: &Path,
) -> Result<PathBuf, BoxError> {
    let digest = seed
        .input_sha256
        .as_deref()
        .ok_or_else(|| format!("the seed {} has no input digest", seed.name))?;
    if dir.exists() {
        fs::remove_dir_all(dir)?;
    }
    let output = Command::new(zakura_sim)
        .args(["seeds", "--sim", &seed.harness, "--set", &seed.set, "--out"])
        .arg(dir)
        .output()?;
    let file = dir.join(digest);
    match fs::read(&file) {
        Ok(bytes) if hex(&Sha256::digest(&bytes)) == digest => Ok(file),
        Ok(_) => Err(format!("{} isn't the seed {}'s input", file.display(), seed.name).into()),
        Err(error) => Err(format!(
            "`zakura-sim seeds --out` didn't write the seed {}'s input: {error}; it {}: {}",
            seed.name,
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into()),
    }
}

#[cfg(all(test, unix))]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use super::*;

    /// `x`'s SHA-256.
    const X: &str = "2d711642b726b04401627ca9fbac32f5c8530fb1903cc4db02258717921a4881";

    /// Writes an executable `body` at `dir/name`, a stand-in `zakura-sim`.
    fn fake(dir: &Path, name: &str, body: &str) -> PathBuf {
        let path = dir.join(name);
        fs::write(
            &path,
            format!("#!/bin/sh\n[ $# -gt 0 ] || exit 0\n{body}\n"),
        )
        .unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
        super::super::wait_until_executable(&path);
        path
    }

    /// The registry reads every set's report, a set with a rejected seed
    /// included, names each admitted seed's harness, refuses a rejected or
    /// unknown seed, and writes an admitted seed's input.
    #[test]
    fn the_registry_reads_every_set() {
        let dir = tempfile::tempdir().unwrap();
        let sim = fake(
            dir.path(),
            "zakura-sim",
            &format!(
                r#"set=; report=; out=; prev=
for arg in "$@"; do
  case "$prev" in --set) set=$arg;; --report) report=$arg;; --out) out=$arg;; esac
  prev=$arg
done
if [ -n "$out" ]; then mkdir -p "$out"; printf x > "$out/{X}"; exit 0; fi
if [ "$set" = known-failure ]; then
  printf '{{"seeds":[{{"status":"rejected","name":"bad","harness":"h","set":"known-failure","reason":"no witness"}}]}}' > "$report"
  exit 1
fi
printf '{{"seeds":[{{"status":"admitted","name":"%s-seed","harness":"h","set":"%s","input_sha256":"{X}"}}]}}' "$set" "$set" > "$report""#
            ),
        );
        let registry = Registry::read(&sim, &dir.path().join("reports")).expect("the reports read");
        assert_eq!(registry.harness("corpus-seed").unwrap(), "h");
        assert_eq!(registry.harness("slow-seed").unwrap(), "h");
        let error = registry.seed("bad").unwrap_err().to_string();
        assert!(error.contains("fails validation: no witness"), "{error}");
        let error = registry.seed("nope").unwrap_err().to_string();
        assert!(error.contains("no seed is named `nope`"), "{error}");
        assert_eq!(registry.input_digests().collect::<Vec<_>>(), [X, X]);
        let input = write_input(
            &sim,
            registry.seed("corpus-seed").unwrap(),
            &dir.path().join("out"),
        )
        .expect("the input is written");
        assert_eq!(fs::read(input).unwrap(), b"x");

        let silent = fake(dir.path(), "silent", "exit 2");
        let error = Registry::read(&silent, &dir.path().join("none"))
            .unwrap_err()
            .to_string();
        assert!(error.contains("wrote no report"), "{error}");
        let row = registry.seed("corpus-seed").unwrap().clone();
        assert!(Registry::from_rows([row.clone(), row]).is_err());
    }
}
