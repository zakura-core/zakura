//! Where the planted cases live, and the property each one bypasses.
//!
//! A case is a bypass patch and its case file, `<name>.patch` and
//! `<name>.toml`, at any depth under [`PLANTED_DIR`]. The case's property ID
//! is its directory relative to [`PLANTED_DIR`]: the case
//! `qa/planted/discovery/s4v2/hello/sender-cadence/repeat-hello.patch`
//! bypasses `discovery/s4v2/hello/sender-cadence`.
//!
//! `cargo xtask planted` includes this file, so the runner and the
//! simulator's own checks find the same cases under the same IDs.

use std::{
    fs, io,
    path::{Path, PathBuf},
};

/// Where the planted cases live, relative to the repository's root.
pub const PLANTED_DIR: &str = "qa/planted";

/// Every file with `extension` under `dir`, at any depth, sorted by path.
/// The planted runner's cases are the `patch` files under the repository's
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
