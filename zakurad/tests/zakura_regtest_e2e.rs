//! Opt-in Zakura dual-stack regtest e2e (lightweight, host-networked).
//!
//! Ignored by default, and additionally skipped in CI unless
//! `ZAKURA_REGTEST_E2E=1` is set. The unit-test workflow opts this test in as
//! a dedicated required job so failures are isolated from ordinary unit-test
//! output. There is no image build: each node runs the host-built `zakurad`
//! binary bind-mounted into stock `debian:trixie-slim`.
//!
//! To run it locally:
//!
//! ```sh
//! cargo test -p zakura --test zakura_regtest_e2e -- --ignored --nocapture
//! ```
//!
//! To force it in any CI environment, set `ZAKURA_REGTEST_E2E=1`.
//!
//! `ZAKURA_E2E_MODE` selects the shell harness mode:
//!
//! - `smoke` (default): the existing four-node flow with a short chain, reset
//!   catch-up, and non-finalized reorg.
//! - `pr-gate`: a trimmed PR/merge-queue confidence gate that derives a short
//!   checkpoint list, forces a from-zero kind-6 catch-up across a checkpoint to
//!   full-verifier handoff, runs the trace oracle as a primary assertion layer,
//!   and writes a compact `timeline.jsonl` artifact.
//! - `checkpoint-long`: mines a 4,000-block Regtest chain and derives
//!   checkpoints every 400 blocks before node2's from-zero catch-up.
//! - `no-checkpoint-long`: mines the same long chain but configures node2 with
//!   `checkpoints = false`, leaving only genesis before full verification.
//! - `restart-matrix`: uses the long checkpoint chain and restarts node2 around
//!   height 0, 399/400/401, 2,000, near tip with a 1,000-block gap, and after
//!   the non-finalized reorg.
//! - `header-faults`: uses a bounded multi-range chain, restarts node2 with
//!   outstanding header-sync work, and requires v8 request-id trace coverage.
//!
//! It shells out to `docker/zakura-regtest-e2e/run.sh`, which builds `zakurad`
//! (debug) if needed, brings up four Regtest nodes sharing the host network — a
//! dual-stack seed, a pure Zakura-only node (`p2p_stack = "zakura"`) that joins
//! only via the seed's `zakura.bootstrap_peers`, a legacy-only node, and a
//! dual-stack node that upgrades — and asserts legacy TCP backwards
//! compatibility, the legacy->Zakura upgrade handshake, and block propagation
//! to the pure-Zakura and legacy-only peers. The pure-Zakura node also disables
//! the Regtest genesis self-seed (`sync.debug_skip_regtest_genesis_self_seed`),
//! so the run additionally proves it bootstraps the genesis block over Zakura
//! from an empty state — the production Mainnet/Testnet bootstrap path that was
//! previously stuck at height 0. After propagation, the pure-Zakura node is reset
//! to an empty state while the seed sits idle at the tip, proving it re-downloads
//! the whole chain over kind-6 block sync (gossip cannot help, since the seed
//! re-advertises nothing) — the production from-0 / restart-catch-up path. The
//! upgraded node4 propagation path
//! remains a documented P2 known issue and can be made fatal with
//! `ZAKURA_REGTEST_E2E_STRICT_UPGRADE=1`. See that script for the exact
//! assertions.

#![allow(clippy::print_stderr)]

use std::{
    path::PathBuf,
    process::Command,
    sync::{Mutex, MutexGuard},
};

/// Serializes the docker e2e lanes: each run owns the shared host network and
/// the per-label container namespace, so two stacks must never overlap.
static E2E_STACK_LOCK: Mutex<()> = Mutex::new(());

#[ignore = "opt-in docker e2e: needs docker (builds the host zakurad binary itself)"]
#[test]
fn zakura_regtest_dual_stack_e2e() {
    // `ZAKURA_E2E_MODE` routes an unfiltered `--ignored` run to exactly one
    // lane per CI job; `header-faults` belongs to the dedicated test below.
    if std::env::var("ZAKURA_E2E_MODE").as_deref() == Ok("header-faults") {
        eprintln!(
            "skipping dual-stack e2e: ZAKURA_E2E_MODE=header-faults runs the header-sync fault lane"
        );
        return;
    }
    run_zakura_regtest_e2e(None);
}

#[ignore = "opt-in docker e2e: header-sync v8 fault lane"]
#[test]
fn zakura_regtest_header_sync_faults() {
    if std::env::var("ZAKURA_E2E_MODE").is_ok_and(|mode| mode != "header-faults") {
        eprintln!(
            "skipping header-sync fault e2e: ZAKURA_E2E_MODE selects the dual-stack lane above"
        );
        return;
    }
    run_zakura_regtest_e2e(Some("header-faults"));
}

fn run_zakura_regtest_e2e(mode: Option<&str>) {
    // The unit-test CI lane runs ignored tests (`--run-ignored=all`) on runners
    // that have Docker, so `#[ignore]` plus the Docker guard below is not enough
    // to keep this host-networked, environment-sensitive e2e out of CI. Skip in
    // CI unless explicitly opted in with `ZAKURA_REGTEST_E2E=1`.
    if std::env::var_os("CI").is_some() && std::env::var_os("ZAKURA_REGTEST_E2E").is_none() {
        eprintln!("skipping Zakura regtest e2e in CI: set ZAKURA_REGTEST_E2E=1 to force it");
        return;
    }

    if !command_succeeds(Command::new("docker").arg("version")) {
        eprintln!("skipping Zakura regtest e2e: docker is unavailable");
        return;
    }

    let _stack: MutexGuard<'_, ()> = E2E_STACK_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    let script =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../docker/zakura-regtest-e2e/run.sh");

    let mut command = Command::new("bash");
    command.arg(&script);
    if let Some(mode) = mode {
        command.env("ZAKURA_E2E_MODE", mode);
    }
    let status = command
        .status()
        .expect("failed to spawn the Zakura regtest e2e script");

    assert!(status.success(), "Zakura regtest e2e failed");
}

fn command_succeeds(command: &mut Command) -> bool {
    command
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false)
}
