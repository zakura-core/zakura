//! Where the test artifacts live, and the property each planted case
//! bypasses.
//!
//! Planted cases, native mutations, scenario fixtures and seeds are test
//! artifacts. They live in the QA store, the `zakura-sim-corpus` Space, not
//! in the repository. `scripts/qa_store.py pull` copies the Space to a local
//! directory, and [`STORE_ENV`] names that directory. Paths inside the
//! store, and the paths a case file names, are relative to its root.
//!
//! A case is a bypass patch and its case file, `<name>.patch` and
//! `<name>.toml`, at any depth under the store's [`PLANTED_DIR`]. The case's
//! property ID is its directory relative to [`PLANTED_DIR`]: the case
//! `qa/planted/discovery/s4v2/hello/sender-cadence/repeat-hello.patch`
//! bypasses `discovery/s4v2/hello/sender-cadence`.
//!
//! `cargo xtask planted` includes this file, so the runner and the
//! simulator's own checks find the same cases under the same IDs.

use std::{
    env, fs, io,
    path::{Path, PathBuf},
};

/// The environment variable that names the QA store's local copy.
pub const STORE_ENV: &str = "ZAKURA_QA_STORE";

/// Where the planted cases live, relative to the store's root.
pub const PLANTED_DIR: &str = "qa/planted";

/// The QA store's local copy, if [`STORE_ENV`] names one.
pub fn store_root() -> Option<PathBuf> {
    env::var_os(STORE_ENV)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

/// [`store_root`], or an error that says how to get a store.
pub fn require_store() -> io::Result<PathBuf> {
    store_root().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!(
                "{STORE_ENV} names no QA store: the test artifacts live in the \
                 zakura-sim-corpus Space, and `scripts/qa_store.py pull --dest <dir>` copies \
                 them to <dir>"
            ),
        )
    })
}

/// Every file with `extension` under `dir`, at any depth, sorted by path.
/// The planted runner's cases are the `patch` files under the store's
/// [`PLANTED_DIR`].
pub fn files(dir: &Path, extension: &str) -> io::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    let mut dirs = vec![dir.to_owned()];
    while let Some(dir) = dirs.pop() {
        let context =
            |error: io::Error| io::Error::new(error.kind(), format!("{}: {error}", dir.display()));
        for entry in fs::read_dir(&dir).map_err(context)? {
            let path = entry.map_err(context)?.path();
            if path.is_dir() {
                dirs.push(path);
            } else if path.extension().is_some_and(|e| e == extension) {
                files.push(path);
            }
        }
    }
    files.sort();
    Ok(files)
}

/// The property ID of the case whose patch or case file is `case`: the
/// case's directory relative to `planted`, the catalogue's root. `None` if
/// that directory isn't strictly under `planted`, or isn't UTF-8.
pub fn property_id(planted: &Path, case: &Path) -> Option<String> {
    case.parent()?
        .strip_prefix(planted)
        .ok()?
        .to_str()
        .filter(|id| !id.is_empty())
        .map(str::to_owned)
}
