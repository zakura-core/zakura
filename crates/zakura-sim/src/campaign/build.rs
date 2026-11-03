//! Fuzzer builds: `cargo fuzz build` with the pinned toolchain, and the
//! build record that says where a fuzzer binary came from.

use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Command,
};

use serde::{Deserialize, Serialize};

use super::store::{
    partial_path, read_record, record_bytes, sync_dir, write_once, CampaignError, Digest, Store,
    STORE_VERSION,
};

#[allow(dead_code)]
#[path = "../../build/provenance.rs"]
mod provenance;

pub use crate::suite::NIGHTLY;

/// The simulator fuzz package, relative to the repository's root.
pub const FUZZ_PACKAGE: &str = "qa/fuzz/sim";

/// The simulator build's flag, which the fuzz package needs.
const SIM_RUSTFLAGS: &str = "--cfg zakura_sim";

/// A build record's file.
const BUILD_FILE: &str = "build.json";

/// The fuzzer binary's file in a build directory.
const FUZZER_FILE: &str = "fuzzer";

/// What a fuzzer binary was built from and how: everything that must match
/// for a run to resume on another build of the same fuzzer.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BuildIdentity {
    /// The fuzz package, [`FUZZ_PACKAGE`].
    pub package: String,
    /// The fuzz target.
    pub target: String,
    /// The registered harness the target runs, from the fuzz package's
    /// `[package.metadata.zakura-sim.targets]` table.
    pub harness: String,
    /// `HEAD`'s commit.
    pub git: String,
    /// True if the checkout had any change under the source paths or the
    /// fuzz package, or if git couldn't say.
    pub dirty: bool,
    /// SHA-256 over every file under the source paths and the fuzz package
    /// that git doesn't ignore, as `zakura-sim`'s build script computes it.
    pub source: String,
    /// SHA-256 of the workspace's `Cargo.lock`.
    pub lockfile: String,
    /// SHA-256 of the fuzz package's `Cargo.lock`.
    pub fuzz_lockfile: String,
    /// `rustc --version` of the toolchain that built the fuzzer.
    pub rustc: String,
    /// `cargo fuzz --version`.
    pub cargo_fuzz: String,
    /// The `libfuzzer-sys` version in the fuzz package's lockfile, which
    /// fixes the libFuzzer sources.
    pub libfuzzer_sys: String,
    /// The `RUSTFLAGS` the build ran with.
    pub rustflags: String,
    /// The arguments after `cargo +<toolchain> fuzz build`, which give the
    /// sanitizer and the features.
    pub cargo_args: Vec<String>,
}

impl BuildIdentity {
    /// The names of the fields in which `self` and `other` differ.
    pub fn differences(&self, other: &BuildIdentity) -> Vec<&'static str> {
        let fields: [(&'static str, bool); 13] = [
            ("package", self.package == other.package),
            ("target", self.target == other.target),
            ("harness", self.harness == other.harness),
            ("git", self.git == other.git),
            ("dirty", self.dirty == other.dirty),
            ("source", self.source == other.source),
            ("lockfile", self.lockfile == other.lockfile),
            ("fuzz_lockfile", self.fuzz_lockfile == other.fuzz_lockfile),
            ("rustc", self.rustc == other.rustc),
            ("cargo_fuzz", self.cargo_fuzz == other.cargo_fuzz),
            ("libfuzzer_sys", self.libfuzzer_sys == other.libfuzzer_sys),
            ("rustflags", self.rustflags == other.rustflags),
            ("cargo_args", self.cargo_args == other.cargo_args),
        ];
        fields
            .into_iter()
            .filter(|(_, same)| !same)
            .map(|(name, _)| name)
            .collect()
    }

    /// True if the build's provenance is known and its tree was clean, so
    /// the identity says what the binary was built from. A dirty build's
    /// recorded source may be stale, as [`BuildInfo::equivalent`] says.
    ///
    /// [`BuildInfo::equivalent`]: crate::trace::BuildInfo::equivalent
    pub fn clean(&self) -> bool {
        !self.dirty
            && [&self.git, &self.source, &self.lockfile, &self.fuzz_lockfile]
                .iter()
                .all(|field| field.as_str() != provenance::UNKNOWN)
    }
}

/// A fuzzer binary in the store, and its identity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FuzzBuild {
    /// [`STORE_VERSION`].
    pub version: u32,
    /// The binary's SHA-256, which names the build. Two builds of one
    /// identity on two hosts can differ here, since the binary embeds
    /// paths.
    pub binary: Digest,
    /// What the binary was built from and how.
    pub identity: BuildIdentity,
}

impl Store {
    /// Saves the fuzzer binary `bytes` in the store with its build record,
    /// and returns the record. Saving one binary twice with one identity
    /// keeps the first copy. Refuses a binary the store already holds with
    /// another identity.
    pub fn save_build(
        &self,
        bytes: &[u8],
        identity: BuildIdentity,
    ) -> Result<FuzzBuild, CampaignError> {
        let build = FuzzBuild {
            version: STORE_VERSION,
            binary: Digest::of(bytes),
            identity,
        };
        let dir = self.build_dir(&build.binary);
        let io = |what: &str| CampaignError::io(format!("{what} {}", dir.display()));
        fs::create_dir_all(&dir).map_err(io("creating"))?;
        let fuzzer = dir.join(FUZZER_FILE);
        if !fuzzer.exists() {
            let partial = partial_path(&dir);
            let copied = fs::write(&partial, bytes)
                .and_then(|()| fs::set_permissions(&partial, fs::Permissions::from_mode(0o755)))
                .and_then(|()| fs::File::open(&partial)?.sync_all())
                .and_then(|()| fs::rename(&partial, &fuzzer))
                .and_then(|()| sync_dir(&dir));
            if copied.is_err() {
                let _ = fs::remove_file(&partial);
            }
            copied.map_err(io("copying the fuzzer into"))?;
        }
        let record = dir.join(BUILD_FILE);
        if !write_once(&record, &record_bytes(&build)).map_err(io("writing the build record in"))? {
            let existing = self.read_build(&build.binary)?;
            if existing != build {
                return Err(CampaignError::Refused(format!(
                    "the store holds fuzzer {} with another identity; these differ: {}",
                    build.binary,
                    existing.identity.differences(&build.identity).join(", ")
                )));
            }
        }
        Ok(build)
    }

    /// Reads the build record of fuzzer `binary`, and checks that the
    /// binary beside it has that digest.
    pub fn read_build(&self, binary: &Digest) -> Result<FuzzBuild, CampaignError> {
        let dir = self.build_dir(binary);
        let record = dir.join(BUILD_FILE);
        let build: FuzzBuild = read_record(&record, None)?;
        if &build.binary != binary {
            return Err(CampaignError::corrupt(
                &record,
                format!("it's under {binary}, and it records {}", build.binary),
            ));
        }
        let fuzzer = dir.join(FUZZER_FILE);
        let actual = Digest::of_file(&fuzzer)
            .map_err(CampaignError::io(format!("reading {}", fuzzer.display())))?;
        if &actual != binary {
            return Err(CampaignError::corrupt(
                &fuzzer,
                format!("its build is {binary}, and its digest is {actual}"),
            ));
        }
        Ok(build)
    }

    /// The fuzzer binary of build `binary`.
    pub fn fuzzer_path(&self, binary: &Digest) -> PathBuf {
        self.build_dir(binary).join(FUZZER_FILE)
    }
}

/// Builds fuzz target `target` of [`FUZZ_PACKAGE`] in the checkout at
/// `repo` with `cargo +NIGHTLY fuzz build`, into `target_dir`, and returns
/// the binary's bytes and its identity. `sanitizer` is cargo-fuzz's
/// `--sanitizer`; `none` also turns off the default features, as the fuzz
/// package's README says.
///
/// Refuses if the checkout changed while cargo built and the binary was
/// read, since the binary may then come from neither state.
pub fn build_fuzzer(
    repo: &Path,
    target: &str,
    sanitizer: &str,
    target_dir: &Path,
) -> Result<(Vec<u8>, BuildIdentity), CampaignError> {
    let package = repo.join(FUZZ_PACKAGE);
    let manifest = package.join("Cargo.toml");
    let manifest_text = fs::read_to_string(&manifest)
        .map_err(CampaignError::io(format!("reading {}", manifest.display())))?;
    let harness = target_harness(&manifest_text, target).ok_or_else(|| {
        CampaignError::Refused(format!(
            "{} maps no harness to fuzz target {target} in [package.metadata.zakura-sim.targets]",
            manifest.display()
        ))
    })?;
    let mut cargo_args: Vec<String> = vec!["--sanitizer".to_owned(), sanitizer.to_owned()];
    if sanitizer == "none" {
        cargo_args.push("--no-default-features".to_owned());
    }
    let rustflags = SIM_RUSTFLAGS.to_owned();
    let (bytes, checkout) = while_unchanged(repo, || {
        let status = toolchain_command("cargo")
            .args(["fuzz", "build", "--fuzz-dir"])
            .arg(&package)
            .arg("--target-dir")
            .arg(target_dir)
            .args(&cargo_args)
            .arg(target)
            .current_dir(&package)
            .env("RUSTFLAGS", &rustflags)
            .status()
            .map_err(CampaignError::io("running cargo fuzz build"))?;
        if !status.success() {
            return Err(CampaignError::Refused(format!(
                "cargo fuzz build of {target} failed with {status}"
            )));
        }
        let host = output(toolchain_command("rustc").arg("-vV"))?
            .lines()
            .find_map(|line| line.strip_prefix("host: ").map(str::to_owned))
            .ok_or_else(|| CampaignError::Refused("`rustc -vV` names no host".to_owned()))?;
        let binary = target_dir.join(host).join("release").join(target);
        fs::read(&binary).map_err(CampaignError::io(format!("reading {}", binary.display())))
    })?;
    let fuzz_lock = package.join("Cargo.lock");
    let lock_text = fs::read_to_string(&fuzz_lock).map_err(CampaignError::io(format!(
        "reading {}",
        fuzz_lock.display()
    )))?;
    let identity = BuildIdentity {
        package: FUZZ_PACKAGE.to_owned(),
        target: target.to_owned(),
        harness,
        git: checkout.git,
        dirty: checkout.dirty,
        source: checkout.source,
        lockfile: checkout.lockfile,
        fuzz_lockfile: checkout.fuzz_lockfile,
        rustc: output(toolchain_command("rustc").arg("--version"))?
            .trim()
            .to_owned(),
        cargo_fuzz: output(toolchain_command("cargo").args(["fuzz", "--version"]))?
            .trim()
            .to_owned(),
        libfuzzer_sys: locked_version(&lock_text, "libfuzzer-sys").ok_or_else(|| {
            CampaignError::Refused(format!("{} locks no libfuzzer-sys", fuzz_lock.display()))
        })?,
        rustflags,
        cargo_args,
    };
    Ok((bytes, identity))
}

/// The state of a checkout that a build record names.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Checkout {
    /// `HEAD`'s commit.
    git: String,
    /// True if the tree had changes under the source paths or the fuzz
    /// package, or if git couldn't say.
    dirty: bool,
    /// The digest of the source paths and the fuzz package.
    source: String,
    /// The digest of the workspace's `Cargo.lock`.
    lockfile: String,
    /// The digest of the fuzz package's `Cargo.lock`.
    fuzz_lockfile: String,
}

impl Checkout {
    /// The checkout at `repo` as it is now.
    fn read(repo: &Path) -> Checkout {
        let mut paths = provenance::SOURCE_PATHS.to_vec();
        paths.push(FUZZ_PACKAGE);
        let provenance = provenance::Provenance::read(repo, &paths);
        let digest = |path: PathBuf| {
            provenance::file_digest(&path).unwrap_or_else(|| provenance::UNKNOWN.to_owned())
        };
        Checkout {
            git: provenance.git,
            dirty: provenance.dirty,
            source: provenance.source,
            lockfile: digest(repo.join("Cargo.lock")),
            fuzz_lockfile: digest(repo.join(FUZZ_PACKAGE).join("Cargo.lock")),
        }
    }
}

/// Runs `build` in the checkout at `repo`, and returns its result with the
/// checkout's state. Refuses if the state changed while `build` ran.
fn while_unchanged<T>(
    repo: &Path,
    build: impl FnOnce() -> Result<T, CampaignError>,
) -> Result<(T, Checkout), CampaignError> {
    let before = Checkout::read(repo);
    let built = build()?;
    let after = Checkout::read(repo);
    if before != after {
        return Err(CampaignError::Refused(format!(
            "the checkout at {} changed during the build, from {before:?} to {after:?}; build \
             again from a tree that doesn't change",
            repo.display()
        )));
    }
    Ok((built, after))
}

/// `program` run with [`NIGHTLY`], with the variables of an enclosing
/// `cargo run` removed, so they can't pick another toolchain or target
/// directory.
fn toolchain_command(program: &str) -> Command {
    let mut command = Command::new(program);
    command.arg(format!("+{NIGHTLY}"));
    for (key, _) in std::env::vars_os() {
        let key = key.to_string_lossy();
        if key == "RUSTUP_TOOLCHAIN" || key == "CARGO_TARGET_DIR" || key == "CARGO_BUILD_TARGET" {
            command.env_remove(&*key);
        }
    }
    command
}

/// `command`'s standard output. Fails if it can't run or fails.
fn output(command: &mut Command) -> Result<String, CampaignError> {
    let shown = format!("{command:?}");
    let output = command
        .output()
        .map_err(CampaignError::io(format!("running {shown}")))?;
    if !output.status.success() {
        return Err(CampaignError::Refused(format!(
            "{shown} failed with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

/// The harness that fuzz target `target` runs, from the
/// `[package.metadata.zakura-sim.targets]` table of the fuzz package's
/// manifest `manifest`, if the table maps one.
fn target_harness(manifest: &str, target: &str) -> Option<String> {
    let manifest: toml::Value = toml::from_str(manifest).ok()?;
    manifest
        .get("package")?
        .get("metadata")?
        .get("zakura-sim")?
        .get("targets")?
        .get(target)?
        .as_str()
        .map(str::to_owned)
}

/// The version of package `name` in the lockfile `lock`, if it locks one.
fn locked_version(lock: &str, name: &str) -> Option<String> {
    let lock: toml::Value = toml::from_str(lock).ok()?;
    lock.get("package")?
        .as_array()?
        .iter()
        .find(|package| package.get("name").and_then(toml::Value::as_str) == Some(name))?
        .get("version")?
        .as_str()
        .map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The fuzz package's lockfile names its `libfuzzer-sys`.
    #[test]
    fn the_fuzz_lockfile_locks_libfuzzer() {
        let lock = include_str!("../../../../qa/fuzz/sim/Cargo.lock");
        let version = locked_version(lock, "libfuzzer-sys").expect("the fuzz package locks it");
        assert!(version.starts_with("0."), "{version}");
        assert_eq!(locked_version(lock, "no-such-package"), None);
    }

    /// A build records the checkout's state only if the state didn't change
    /// while it built.
    #[test]
    fn a_checkout_that_changes_during_the_build_is_refused() {
        let repo = tempfile::TempDir::new().unwrap();
        let git = |args: &[&str]| {
            let status = Command::new("git")
                .args(args)
                .current_dir(repo.path())
                .status()
                .unwrap();
            assert!(status.success(), "git {args:?}");
        };
        git(&["init", "-q"]);
        let source = repo.path().join("crates/a.rs");
        fs::create_dir_all(source.parent().unwrap()).unwrap();
        fs::write(&source, "fn a() {}").unwrap();
        let (built, checkout) = while_unchanged(repo.path(), || Ok(1)).unwrap();
        assert_eq!(built, 1);
        assert_eq!(checkout, Checkout::read(repo.path()));
        let refused = while_unchanged(repo.path(), || {
            fs::write(&source, "fn b() {}").unwrap();
            Ok(())
        })
        .unwrap_err();
        assert!(
            refused.to_string().contains("changed during the build"),
            "{refused}"
        );
    }

    /// Each fuzz target the package maps runs a registered harness. The
    /// suite check requires every other target to be excluded in
    /// `qa/suite.toml`, as `feedback_probe` is.
    #[test]
    fn every_mapped_fuzz_target_runs_a_registered_harness() {
        let text = include_str!("../../../../qa/fuzz/sim/Cargo.toml");
        let manifest: toml::Value = toml::from_str(text).unwrap();
        let targets = manifest["bin"].as_array().unwrap();
        let mut mapped = 0;
        for target in targets {
            let name = target["name"].as_str().unwrap();
            if let Some(harness) = target_harness(text, name) {
                crate::harness::find(&harness).unwrap();
                mapped += 1;
            }
        }
        assert!(mapped > 0);
        assert_eq!(target_harness(text, "no_such_target"), None);
    }
}
