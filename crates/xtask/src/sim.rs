//! `cargo xtask record`, `cargo xtask replay`, `cargo xtask shrink`,
//! `cargo xtask sim-versions`, `cargo xtask sim-seeds` and `cargo xtask
//! sim-suite`: run the `zakura-sim` binary in the simulator build.
//!
//! xtask is a normal build, so it can't link `zakura-sim`, which only exists
//! behind `--cfg zakura_sim`. It runs the binary through `cargo run` instead,
//! with the flag and target directory CI's `sim-build` job uses, so the two
//! share a build cache.

use std::{
    env,
    error::Error,
    ffi::{OsStr, OsString},
    fmt,
    path::Path,
    process::Command,
};

use crate::{render_command_line, BoxError, CommandError};

/// The simulator build's flag.
pub(super) const SIM_RUSTFLAGS: &str = "--cfg zakura_sim";

/// A `zakura-sim` run that exited with `code`, which xtask exits with too:
/// the binary's codes are its contract, 2 for a usage error and 1 for a
/// failed run.
#[derive(Debug)]
pub(super) struct SimExit {
    pub(super) code: i32,
    error: CommandError,
}

impl fmt::Display for SimExit {
    /// The failed command and its status.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(f)
    }
}

impl Error for SimExit {}

/// Runs `zakura-sim <subcommand> <args>` from the workspace at `repo_root`,
/// passing the arguments through unchanged. Relative paths in them resolve
/// against the current directory. A nonzero exit fails with [`SimExit`].
///
/// The command line goes to stderr, so stdout is the binary's own output,
/// such as `sim-versions`' `KEY=VALUE` lines.
pub(super) fn run(
    repo_root: &Path,
    subcommand: &str,
    args: impl Iterator<Item = String>,
) -> Result<(), BoxError> {
    let cargo = env::var_os("CARGO").unwrap_or_else(|| OsString::from("cargo"));
    let mut command = Command::new(cargo);
    command
        .env("RUSTFLAGS", SIM_RUSTFLAGS)
        .env("CARGO_TARGET_DIR", repo_root.join("target").join("sim"))
        .arg("run")
        .arg("--locked")
        .arg("--manifest-path")
        .arg(repo_root.join("Cargo.toml"))
        .args(["-p", "zakura-sim", "--bin", "zakura-sim", "--"])
        .arg(subcommand)
        .args(args);
    let args: Vec<OsString> = command.get_args().map(OsStr::to_owned).collect();
    eprintln!("$ {}", render_command_line(command.get_program(), &args));
    let status = command.status()?;
    if status.success() {
        return Ok(());
    }
    let error = CommandError::new(&command, status);
    Err(match status.code() {
        Some(code) => Box::new(SimExit { code, error }),
        None => Box::new(error),
    })
}
