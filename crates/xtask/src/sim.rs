//! `cargo xtask record`: runs the `zakura-sim` binary in the simulator build.
//!
//! xtask is a normal build, so it can't link `zakura-sim`, which only exists
//! behind `--cfg zakura_sim`. It runs the binary through `cargo run` instead,
//! with the flag and target directory CI's `sim-build` job uses, so the two
//! share a build cache.

use std::{env, ffi::OsString, path::Path, process::Command};

use crate::{run_command, BoxError};

/// The simulator build's flag.
const SIM_RUSTFLAGS: &str = "--cfg zakura_sim";

/// Runs `zakura-sim <subcommand> <args>` from the workspace at `repo_root`,
/// passing the arguments through unchanged. Relative paths in them resolve
/// against the current directory.
pub(super) fn run(
    repo_root: &Path,
    subcommand: &str,
    args: impl Iterator<Item = String>,
) -> Result<(), BoxError> {
    let cargo = env::var_os("CARGO").unwrap_or_else(|| OsString::from("cargo"));
    run_command(
        Command::new(cargo)
            .env("RUSTFLAGS", SIM_RUSTFLAGS)
            .env("CARGO_TARGET_DIR", repo_root.join("target").join("sim"))
            .arg("run")
            .arg("--locked")
            .arg("--manifest-path")
            .arg(repo_root.join("Cargo.toml"))
            .args(["-p", "zakura-sim", "--bin", "zakura-sim", "--"])
            .arg(subcommand)
            .args(args),
    )
}
