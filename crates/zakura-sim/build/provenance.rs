//! Where a build's source came from, for trace `meta` records.
//!
//! `build.rs` uses this, and so does the provenance test, which runs it on a
//! scratch repository.

use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use sha2::{Digest, Sha256};

/// What a build records when it can't read a value.
pub const UNKNOWN: &str = "unknown";

/// The paths, relative to the repository root, whose contents can change a
/// workspace build: every crate, the workspace manifest and lockfile, Cargo's
/// config, and the toolchain pin. `source` and `dirty` cover exactly these,
/// and `build.rs` reruns when any of them changes. Docs, CI and deploy files
/// can't change the binary, so they're left out.
pub const SOURCE_PATHS: &[&str] = &[
    "crates",
    "Cargo.toml",
    "Cargo.lock",
    ".cargo",
    "rust-toolchain.toml",
];

/// A checkout's git state and a digest of its build inputs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Provenance {
    /// `HEAD`'s commit, or [`UNKNOWN`].
    pub git: String,
    /// True if `git status` lists any change under the source paths,
    /// untracked files included, or if git couldn't say.
    pub dirty: bool,
    /// SHA-256 over every tracked and untracked file under the source paths
    /// that git doesn't ignore, or [`UNKNOWN`].
    pub source: String,
    /// The git dir, which changes on every stage, commit and checkout. `None`
    /// when git couldn't name it.
    pub git_dir: Option<PathBuf>,
}

impl Provenance {
    /// Reads the provenance of the checkout that holds `dir`, which may be
    /// any directory inside it, over `paths` relative to its root. A failed
    /// git command leaves its field [`UNKNOWN`] and marks the build dirty, so
    /// unknown provenance never counts as clean.
    pub fn read(dir: &Path, paths: &[&str]) -> Provenance {
        let head = git(dir, &["rev-parse", "HEAD"]);
        let git_dir = git(dir, &["rev-parse", "--absolute-git-dir"]);
        // Every path-listing command runs from the root, since git lists
        // paths relative to the directory it runs in.
        let top = git(dir, &["rev-parse", "--show-toplevel"]).map(|top| PathBuf::from(top.trim()));
        let (status, source) = match &top {
            Some(top) => {
                let status = git(top, &[&["status", "--porcelain", "--"], paths].concat());
                let files = git(
                    top,
                    &[
                        &[
                            "ls-files",
                            "-z",
                            "--cached",
                            "--others",
                            "--exclude-standard",
                            "--",
                        ],
                        paths,
                    ]
                    .concat(),
                );
                (status, files.and_then(|files| source_digest(top, &files)))
            }
            None => (None, None),
        };
        Provenance {
            dirty: head.is_none() || status.is_none_or(|status| !status.is_empty()),
            git: head.map_or_else(|| UNKNOWN.to_owned(), |sha| sha.trim().to_owned()),
            source: source.unwrap_or_else(|| UNKNOWN.to_owned()),
            git_dir: git_dir.map(|dir| PathBuf::from(dir.trim())),
        }
    }
}

/// The lowercase hex SHA-256 of `path`'s contents, or `None` if it can't be
/// read.
pub fn file_digest(path: &Path) -> Option<String> {
    fs::read(path).ok().map(|bytes| hex(&Sha256::digest(bytes)))
}

/// SHA-256 over the NUL-separated `files`, relative to `top`, in sorted
/// order. Each entry hashes its path, then a kind byte and its contents: `F`
/// and the bytes for a file, `L` and the target for a symlink, `D` for a
/// directory such as a submodule, and `-` for a tracked file deleted from the
/// work tree. `None` if a file can't be read.
fn source_digest(top: &Path, files: &str) -> Option<String> {
    let mut paths: Vec<&str> = files.split('\0').filter(|p| !p.is_empty()).collect();
    paths.sort_unstable();
    paths.dedup();
    let mut hasher = Sha256::new();
    for path in paths {
        hasher.update(path.as_bytes());
        hasher.update([0]);
        let full = top.join(path);
        let contents = match fs::symlink_metadata(&full) {
            Ok(meta) if meta.is_symlink() => {
                hasher.update(b"L");
                fs::read_link(&full)
                    .ok()?
                    .into_os_string()
                    .into_encoded_bytes()
            }
            Ok(meta) if meta.is_dir() => {
                hasher.update(b"D");
                Vec::new()
            }
            Ok(_) => {
                hasher.update(b"F");
                fs::read(&full).ok()?
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                hasher.update(b"-");
                Vec::new()
            }
            Err(_) => return None,
        };
        // A usize always fits in a u64 on supported targets.
        hasher.update((contents.len() as u64).to_le_bytes());
        hasher.update(&contents);
    }
    Some(hex(&hasher.finalize()))
}

/// Runs git in `dir` and returns its stdout, or `None` if it failed.
fn git(dir: &Path, args: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        // Keeps `git status` from rewriting the index, which is in the
        // watched git dir and would rerun the build script forever.
        .env("GIT_OPTIONAL_LOCKS", "0")
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout).ok()
}

/// `bytes` as lowercase hex.
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
