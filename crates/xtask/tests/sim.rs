//! `cargo xtask record` and `cargo xtask replay` exit with the `zakura-sim`
//! binary's status, so a usage error stays 2.
#![cfg(unix)]

use std::{os::unix::fs::PermissionsExt, process::Command};

/// Runs `cargo xtask <args>` with a stand-in for cargo that exits `code`,
/// as `zakura-sim` does after `cargo run` builds it. Returns xtask's exit
/// code.
fn xtask_with_child_exit(code: i32, args: &[&str]) -> Option<i32> {
    let dir = tempfile::tempdir().unwrap();
    let cargo = dir.path().join("cargo");
    std::fs::write(&cargo, format!("#!/bin/sh\nexit {code}\n")).unwrap();
    std::fs::set_permissions(&cargo, std::fs::Permissions::from_mode(0o755)).unwrap();
    Command::new(env!("CARGO_BIN_EXE_xtask"))
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
