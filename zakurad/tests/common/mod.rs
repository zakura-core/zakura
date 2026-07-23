//! Shared code for the `zakurad` acceptance tests.
//!
//! # Warning
//!
//! Test functions in this file and its submodules will not be run.
//! This file is only for test library code.
//!
//! This module uses the legacy directory structure,
//! to avoid compiling an empty "common" test binary:
//! <https://doc.rust-lang.org/book/ch11-03-test-organization.html#submodules-in-integration-tests>

#![allow(dead_code)]

pub mod cached_state;
pub mod check;
#[cfg(feature = "zakura-checkpoints")]
pub mod checkpoints;
pub mod coinbase;
pub mod config;
pub mod failure_messages;
pub mod get_block_template_rpcs;
pub mod launch;
pub mod lightwalletd;
pub mod regtest;
pub mod sync;
pub mod test_type;
pub mod zcashd_compat;

/// Returns the path to the compiled `zakurad` binary under test.
///
/// Prefers the value the test runner exports at run time
/// (`CARGO_BIN_EXE_zakurad`), so the tests keep working when the binaries are
/// relocated away from the directory they were compiled in — for example when
/// they are run from a `cargo nextest` archive on a different job than the one
/// that built them. Falls back to the path Cargo bakes in at compile time for
/// ordinary in-tree `cargo test` runs, where the run-time variable is absent.
fn zakurad_exe_path() -> String {
    std::env::var("CARGO_BIN_EXE_zakurad")
        .unwrap_or_else(|_| env!("CARGO_BIN_EXE_zakurad").to_string())
}
