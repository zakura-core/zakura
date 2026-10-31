//! `cargo xtask record` and `cargo xtask replay` exit with the `zakura-sim`
//! binary's status, so a usage error stays 2.
#![cfg(unix)]

use std::{os::unix::fs::PermissionsExt, process::Command};

/// The xtask binary under test. The run-time variable comes first, since
/// nextest sets it when it runs tests from an archive built elsewhere, where
/// the compile-time path doesn't exist.
fn xtask_exe() -> String {
    std::env::var("CARGO_BIN_EXE_xtask").unwrap_or_else(|_| env!("CARGO_BIN_EXE_xtask").to_owned())
}

/// Runs `cargo xtask <args>` with a stand-in for cargo that exits `code`,
/// as `zakura-sim` does after `cargo run` builds it. Returns xtask's exit
/// code.
fn xtask_with_child_exit(code: i32, args: &[&str]) -> Option<i32> {
    let dir = tempfile::tempdir().unwrap();
    let cargo = dir.path().join("cargo");
    std::fs::write(&cargo, format!("#!/bin/sh\nexit {code}\n")).unwrap();
    std::fs::set_permissions(&cargo, std::fs::Permissions::from_mode(0o755)).unwrap();
    Command::new(xtask_exe())
        .args(args)
        .env("CARGO", &cargo)
        .output()
        .unwrap()
        .status
        .code()
}

#[test]
fn sim_wrappers_keep_the_child_exit_code() {
    for subcommand in ["record", "replay"] {
        for code in [0, 1, 2] {
            assert_eq!(
                xtask_with_child_exit(code, &[subcommand]),
                Some(code),
                "{subcommand} with a child exit of {code}"
            );
        }
    }
}
