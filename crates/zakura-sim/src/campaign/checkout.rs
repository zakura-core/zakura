//! The state of a checkout that a fuzz build record names.
//!
//! `build.rs` reads it into each build's identity. `cargo xtask planted
//! pilot` includes this file to read the identity a build of its checkout
//! must have. Its parent module provides `provenance`, the simulator build
//! script's `build/provenance.rs`.

use std::path::{Path, PathBuf};

use super::provenance;

/// The simulator fuzz package, relative to the repository's root.
pub const FUZZ_PACKAGE: &str = "qa/fuzz/sim";

/// The state of a checkout that a build record names.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Checkout {
    /// `HEAD`'s commit.
    pub git: String,
    /// True if the tree had changes under the source paths or the fuzz
    /// package, or if git couldn't say.
    pub dirty: bool,
    /// The digest of the source paths and the fuzz package.
    pub source: String,
    /// The digest of the workspace's `Cargo.lock`.
    pub lockfile: String,
    /// The digest of the fuzz package's `Cargo.lock`.
    pub fuzz_lockfile: String,
}

impl Checkout {
    /// The checkout at `repo` as it is now.
    pub fn read(repo: &Path) -> Checkout {
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
