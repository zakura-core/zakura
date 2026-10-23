//! Build script for `zakura-sim`.
//!
//! Records the build's provenance as environment variables that
//! `trace::meta::BuildInfo::current` compiles in, so a trace's `meta` record
//! names the build that wrote it.

use std::{
    env,
    path::{Path, PathBuf},
    process::Command,
};

#[path = "build/provenance.rs"]
mod provenance;

use provenance::{file_digest, Provenance, SOURCE_PATHS, UNKNOWN};

/// Separates list entries in one environment variable. cfg values may hold
/// commas and spaces, but not this.
const SEPARATOR: char = '\u{1f}';

/// Process entry point for `zakura-sim`'s build script.
fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=build");
    // The crate is empty outside the simulator build, so nothing reads these.
    if env::var_os("CARGO_CFG_ZAKURA_SIM").is_none() {
        return;
    }

    let manifest = PathBuf::from(required("CARGO_MANIFEST_DIR"));
    let workspace = manifest
        .parent()
        .and_then(Path::parent)
        .expect("the crate lives at crates/zakura-sim in the workspace");
    let lockfile = workspace.join("Cargo.lock");
    let provenance = Provenance::read(&manifest, SOURCE_PATHS);
    if let Some(git_dir) = &provenance.git_dir {
        rerun_if_changed(git_dir);
    }
    for path in SOURCE_PATHS {
        let path = workspace.join(path);
        // Cargo reruns a script on every build while a watched path is
        // missing.
        if path.exists() {
            rerun_if_changed(&path);
        }
    }

    emit("GIT", &provenance.git);
    emit("DIRTY", if provenance.dirty { "1" } else { "0" });
    emit("SOURCE", &provenance.source);
    emit(
        "LOCKFILE",
        &file_digest(&lockfile).unwrap_or_else(|| UNKNOWN.to_owned()),
    );
    emit("RUSTC", &rustc_version());
    emit("TARGET", &required("TARGET"));
    emit("OPT_LEVEL", &required("OPT_LEVEL"));
    emit("CFG", &join(cfg_set()));
    emit("FEATURES", &join(features()));
    emit(
        "SANITIZER",
        &env::var("CARGO_CFG_SANITIZE").unwrap_or_default(),
    );
}

/// The target's cfg set, as `name` or `name="value"` entries, sorted. It
/// leaves out `sanitize`, which `BuildInfo` records on its own, and
/// `feature`, which [`features`] covers.
fn cfg_set() -> Vec<String> {
    let mut cfg = Vec::new();
    for (key, value) in env::vars() {
        let Some(name) = key.strip_prefix("CARGO_CFG_") else {
            continue;
        };
        let name = name.to_lowercase();
        if name == "sanitize" || name == "feature" {
            continue;
        }
        if value.is_empty() {
            cfg.push(name);
        } else {
            cfg.extend(value.split(',').map(|v| format!("{name}=\"{v}\"")));
        }
    }
    cfg.sort_unstable();
    cfg
}

/// The crate's enabled features, lowercase and sorted.
fn features() -> Vec<String> {
    let mut features: Vec<String> = env::vars()
        .filter_map(|(key, _)| key.strip_prefix("CARGO_FEATURE_").map(str::to_lowercase))
        .collect();
    features.sort_unstable();
    features
}

/// `$RUSTC --version`, or [`UNKNOWN`].
fn rustc_version() -> String {
    Command::new(required("RUSTC"))
        .arg("--version")
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map_or_else(|| UNKNOWN.to_owned(), |v| v.trim().to_owned())
}

/// `entries` joined by [`SEPARATOR`].
fn join(entries: Vec<String>) -> String {
    entries.join(&SEPARATOR.to_string())
}

/// Sets `ZAKURA_SIM_BUILD_<name>` for the crate's compile.
fn emit(name: &str, value: &str) {
    println!("cargo:rustc-env=ZAKURA_SIM_BUILD_{name}={value}");
}

/// Reruns this script when anything under `path` changes.
fn rerun_if_changed(path: &Path) {
    println!("cargo:rerun-if-changed={}", path.display());
}

/// An environment variable Cargo always sets for build scripts.
fn required(name: &str) -> String {
    env::var(name).unwrap_or_else(|_| panic!("Cargo sets {name} for build scripts"))
}
