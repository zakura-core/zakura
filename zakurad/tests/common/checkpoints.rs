//! Test generating checkpoints using `zakura-checkpoints` directly connected to `zakurad`.
//!
//! This test requires a cached chain state that is synchronized close to the network chain tip
//! height. It will finish the sync and update the cached chain state.

use std::{
    env, fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, Ordering},
};

use color_eyre::eyre::Result;
use tempfile::TempDir;

use zakura_chain::{
    block::{Height, HeightDiff, TryIntoHeight},
    parameters::Network,
    transparent::MIN_TRANSPARENT_COINBASE_MATURITY,
};
use zakura_consensus::MAX_CHECKPOINT_HEIGHT_GAP;
use zakura_node_services::rpc_client::RpcRequestClient;
use zakura_state::state_database_format_version_in_code;
use zakura_test::{
    args,
    command::{Arguments, TestDirExt, NO_MATCHES_REGEX_ITER},
    prelude::TestChild,
};

use crate::common::{
    cached_state::{wait_for_state_version_message, wait_for_state_version_upgrade},
    launch::spawn_zakurad_for_rpc,
    sync::{CHECKPOINT_VERIFIER_REGEX, SYNC_FINISHED_REGEX},
    test_type::TestType::*,
};

use super::{
    config::testdir,
    failure_messages::{
        PROCESS_FAILURE_MESSAGES, ZAKURA_CHECKPOINTS_FAILURE_MESSAGES, ZAKURA_FAILURE_MESSAGES,
    },
    launch::ZakuradTestDirExt,
    test_type::TestType,
};

/// The environmental variable used to activate zakurad logs in the checkpoint generation test.
///
/// We use a constant so the compiler detects typos.
pub const LOG_ZAKURAD_CHECKPOINTS: &str = "LOG_ZAKURAD_CHECKPOINTS";

/// The test entry point.
#[allow(clippy::print_stdout)]
pub async fn run(network: Network) -> Result<()> {
    let _init_guard = zakura_test::init();

    // We want a Zakura state dir, but we don't need `lightwalletd`.
    let test_type = UpdateZebraCachedStateWithRpc;
    let test_name = "zakura_checkpoints_test";

    // Skip the test unless the user supplied the correct cached state env vars
    let Some(zakurad_state_path) = test_type.zakurad_state_path(test_name) else {
        return Ok(());
    };

    tracing::info!(
        ?network,
        ?test_type,
        ?zakurad_state_path,
        "running zakura_checkpoints test, spawning zakurad...",
    );

    // Sync zakurad to the network chain tip
    let (mut zakurad, zakura_rpc_address) = if let Some(zakurad_and_address) =
        spawn_zakurad_for_rpc(network.clone(), test_name, test_type, true)?
    {
        zakurad_and_address
    } else {
        // Skip the test, we don't have the required cached state
        return Ok(());
    };

    // Wait for the upgrade if needed.
    // Currently we only write an image for testnet, which is quick.
    // (Mainnet would need to wait at the end of this function, if the upgrade is long.)
    if network.is_a_test_network() {
        let state_version_message = wait_for_state_version_message(&mut zakurad)?;

        // Before we write a cached state image, wait for a database upgrade.
        //
        // It is ok if the logs are in the wrong order and the test sometimes fails,
        // because testnet is unreliable anyway.
        //
        // TODO: this line will hang if the state upgrade is slower than the RPC server spawn.
        // But that is unlikely, because both 25.1 and 25.2 are quick on testnet.
        //
        // TODO: combine this check with the CHECKPOINT_VERIFIER_REGEX and RPC endpoint checks.
        // This is tricky because we need to get the last checkpoint log.
        wait_for_state_version_upgrade(
            &mut zakurad,
            &state_version_message,
            state_database_format_version_in_code(),
            None,
        )?;
    }

    let zakura_rpc_address =
        zakura_rpc_address.expect("zakura_checkpoints test must have RPC port");

    tracing::info!(
        ?network,
        ?zakura_rpc_address,
        "spawned zakurad, waiting for it to load compiled-in checkpoints...",
    );

    let last_checkpoint = zakurad.expect_stdout_line_matches(CHECKPOINT_VERIFIER_REGEX)?;

    // TODO: do this with a regex?
    let (_prefix, last_checkpoint) = last_checkpoint
        .split_once("max_checkpoint_height")
        .expect("just checked log format");
    let (_prefix, last_checkpoint) = last_checkpoint
        .split_once('(')
        .expect("unexpected log format");
    let (last_checkpoint, _suffix) = last_checkpoint
        .split_once(')')
        .expect("unexpected log format");

    tracing::info!(
        ?network,
        ?zakura_rpc_address,
        ?last_checkpoint,
        "found zakurad's current last checkpoint",
    );

    tracing::info!(
        ?network,
        ?zakura_rpc_address,
        "waiting for zakurad to open its RPC port...",
    );
    zakurad.expect_stdout_line_matches(format!("Opened RPC endpoint at {zakura_rpc_address}"))?;

    tracing::info!(
        ?network,
        ?zakura_rpc_address,
        "zakurad opened its RPC port, waiting for it to sync...",
    );

    zakurad.expect_stdout_line_matches(SYNC_FINISHED_REGEX)?;

    let zakura_tip_height = zakurad_tip_height(zakura_rpc_address).await?;
    tracing::info!(
        ?network,
        ?zakura_rpc_address,
        ?zakura_tip_height,
        ?last_checkpoint,
        "zakurad synced to the tip, launching zakura-checkpoints...",
    );

    let zakura_checkpoints = spawn_zakura_checkpoints_direct(
        network.clone(),
        test_type,
        zakura_rpc_address,
        last_checkpoint,
    )?;

    let show_zakurad_logs = env::var(LOG_ZAKURAD_CHECKPOINTS).is_ok();
    if !show_zakurad_logs {
        tracing::info!(
            "zakurad logs are hidden, show them using {LOG_ZAKURAD_CHECKPOINTS}=1 and RUST_LOG=debug"
        );
    }

    tracing::info!(
        ?network,
        ?zakura_rpc_address,
        ?zakura_tip_height,
        ?last_checkpoint,
        "spawned zakura-checkpoints connected to zakurad, checkpoints should appear here...",
    );
    println!("\n\n");

    let (_zakura_checkpoints, _zakurad) = wait_for_zakura_checkpoints_generation(
        zakura_checkpoints,
        zakurad,
        zakura_tip_height,
        test_type,
        show_zakurad_logs,
    )?;

    println!("\n\n");
    tracing::info!(
        ?network,
        ?zakura_tip_height,
        ?last_checkpoint,
        "finished generating Zakura checkpoints",
    );

    Ok(())
}

/// Spawns a `zakura-checkpoints` instance on `network`, connected to `zakurad_rpc_address`.
///
/// Returns:
/// - `Ok(zakura_checkpoints)` on success,
/// - `Err(_)` if spawning `zakura-checkpoints` fails.
#[tracing::instrument]
pub fn spawn_zakura_checkpoints_direct(
    network: Network,
    test_type: TestType,
    zakurad_rpc_address: SocketAddr,
    last_checkpoint: &str,
) -> Result<TestChild<TempDir>> {
    let zakurad_rpc_address = zakurad_rpc_address.to_string();

    let arguments = args![
        "--addr": zakurad_rpc_address,
        "--last-checkpoint": last_checkpoint,
    ];

    // TODO: add logs for different kinds of zakura_checkpoints failures
    let zakura_checkpoints_failure_messages = PROCESS_FAILURE_MESSAGES
        .iter()
        .chain(ZAKURA_FAILURE_MESSAGES)
        .chain(ZAKURA_CHECKPOINTS_FAILURE_MESSAGES)
        .cloned();
    let zakura_checkpoints_ignore_messages = NO_MATCHES_REGEX_ITER.iter().cloned();

    // Currently unused, but we might put a copy of the checkpoints file in it later
    let zakura_checkpoints_dir = testdir()?;

    let mut zakura_checkpoints = zakura_checkpoints_dir
        .spawn_zakura_checkpoints_child(arguments)?
        .with_timeout(test_type.zakurad_timeout())
        .with_failure_regex_iter(
            zakura_checkpoints_failure_messages,
            zakura_checkpoints_ignore_messages,
        );

    // zakura-checkpoints logs to stderr when it launches.
    //
    // This log happens very quickly, so it is ok to block for a short while here.
    zakura_checkpoints.expect_stderr_line_matches(regex::escape("calculating checkpoints"))?;

    Ok(zakura_checkpoints)
}

/// Extension trait for methods on `tempfile::TempDir` for using it as a test
/// directory for `zakura-checkpoints`.
pub trait ZakuraCheckpointsTestDirExt: ZakuradTestDirExt
where
    Self: AsRef<Path> + Sized,
{
    /// Spawn `zakura-checkpoints` with `extra_args`, as a child process in this test directory,
    /// potentially taking ownership of the tempdir for the duration of the child process.
    ///
    /// By default, launch an instance that connects directly to `zakurad`.
    fn spawn_zakura_checkpoints_child(self, extra_args: Arguments) -> Result<TestChild<Self>>;
}

impl ZakuraCheckpointsTestDirExt for TempDir {
    #[allow(clippy::unwrap_in_result)]
    fn spawn_zakura_checkpoints_child(mut self, extra_args: Arguments) -> Result<TestChild<Self>> {
        // By default, launch an instance that connects directly to `zakurad`.
        let mut args = Arguments::new();
        args.set_parameter("--transport", "direct");

        // Apply user provided arguments
        args.merge_with(extra_args);

        // Create debugging info
        let temp_dir = self.as_ref().display().to_string();

        // Try searching the system $PATH first, that's what the test Docker image uses
        let zakura_checkpoints_path = "zakura-checkpoints";

        // Make sure we have the right zakura-checkpoints binary.
        //
        // When we were creating this test, we spent a lot of time debugging a build issue where
        // `zakura-checkpoints` had an empty `main()` function. This check makes sure that doesn't
        // happen again.
        let debug_checkpoints = env::var(LOG_ZAKURAD_CHECKPOINTS).is_ok();
        if debug_checkpoints {
            let mut args = Arguments::new();
            args.set_argument("--help");

            let help_dir = testdir()?;

            tracing::info!(
                ?zakura_checkpoints_path,
                ?args,
                ?help_dir,
                system_path = ?env::var("PATH"),
                // TODO: disable when the tests are working well
                usr_local_zebra_checkpoints_info = ?fs::metadata("/usr/local/bin/zakura-checkpoints"),
                "Trying to launch `zakura-checkpoints --help` by searching system $PATH...",
            );

            let zakura_checkpoints =
                help_dir.spawn_child_with_command(zakura_checkpoints_path, args);

            if let Err(help_error) = zakura_checkpoints {
                tracing::info!(?help_error, "Failed to launch `zakura-checkpoints --help`");
            } else {
                tracing::info!("Launched `zakura-checkpoints --help`, output is:");

                let mut zakura_checkpoints = zakura_checkpoints.unwrap();
                let mut output_is_empty = true;

                // Get the help output
                while zakura_checkpoints.wait_for_stdout_line(None) {
                    output_is_empty = false;
                }
                while zakura_checkpoints.wait_for_stderr_line(None) {
                    output_is_empty = false;
                }

                if output_is_empty {
                    tracing::info!(
                        "`zakura-checkpoints --help` did not log any output. \
                         Is the binary being built during tests? Are its required-features active?"
                    );
                }
            }
        }

        // Try the `zakura-checkpoints` binary the Docker image copied just after it built the tests.
        tracing::info!(
            ?zakura_checkpoints_path,
            ?args,
            ?temp_dir,
            system_path = ?env::var("PATH"),
            // TODO: disable when the tests are working well
            usr_local_zebra_checkpoints_info = ?fs::metadata("/usr/local/bin/zakura-checkpoints"),
            "Trying to launch zakura-checkpoints by searching system $PATH...",
        );

        let zakura_checkpoints =
            self.spawn_child_with_command(zakura_checkpoints_path, args.clone());

        let Err(system_path_error) = zakura_checkpoints else {
            return zakura_checkpoints;
        };

        // Fall back to assuming zakura-checkpoints is in the same directory as zakurad.
        let mut zakura_checkpoints_path: PathBuf = super::zakurad_exe_path().into();
        assert!(
            zakura_checkpoints_path.pop(),
            "must have at least one path component",
        );
        zakura_checkpoints_path.push("zakura-checkpoints");

        if zakura_checkpoints_path.exists() {
            // Create a new temporary directory, because the old one has been used up.
            //
            // TODO: instead, return the TempDir from spawn_child_with_command() on error.
            self = testdir()?;

            // Create debugging info
            let temp_dir = self.as_ref().display().to_string();

            tracing::info!(
                ?zakura_checkpoints_path,
                ?args,
                ?temp_dir,
                ?system_path_error,
                // TODO: disable when the tests are working well
                zakura_checkpoints_info = ?fs::metadata(&zakura_checkpoints_path),
                "Launching from system $PATH failed, \
                 trying to launch zakura-checkpoints from cargo path...",
            );

            self.spawn_child_with_command(
                zakura_checkpoints_path.to_str().expect(
                    "internal test harness error: path is not UTF-8 \
                     TODO: change spawn child methods to take &OsStr not &str",
                ),
                args,
            )
        } else {
            tracing::info!(
                cargo_path = ?zakura_checkpoints_path,
                ?system_path_error,
                // TODO: disable when the tests are working well
                cargo_path_info = ?fs::metadata(&zakura_checkpoints_path),
                "Launching from system $PATH failed, \
                 and zakura-checkpoints cargo path does not exist...",
            );

            // Return the original error
            Err(system_path_error)
        }
    }
}

/// Wait for `zakura-checkpoints` to generate checkpoints, clearing Zebra's logs at the same time.
#[tracing::instrument]
pub fn wait_for_zakura_checkpoints_generation<
    P: ZakuradTestDirExt + std::fmt::Debug + std::marker::Send + 'static,
>(
    mut zakura_checkpoints: TestChild<TempDir>,
    mut zakurad: TestChild<P>,
    zakura_tip_height: Height,
    test_type: TestType,
    show_zakurad_logs: bool,
) -> Result<(TestChild<TempDir>, TestChild<P>)> {
    let last_checkpoint_gap = HeightDiff::from(MIN_TRANSPARENT_COINBASE_MATURITY)
        + HeightDiff::try_from(MAX_CHECKPOINT_HEIGHT_GAP)?;
    let expected_final_checkpoint_height =
        (zakura_tip_height - last_checkpoint_gap).expect("network tip is high enough");

    let is_zakura_checkpoints_finished = AtomicBool::new(false);
    let is_zakura_checkpoints_finished = &is_zakura_checkpoints_finished;

    // Check Zebra's logs for errors.
    //
    // Checkpoint generation can take a long time, so we need to check `zakurad` for errors
    // in parallel.
    let zakurad_mut = &mut zakurad;
    let zakurad_wait_fn = || -> Result<_> {
        tracing::debug!(
            ?test_type,
            "zakurad is waiting for zakura-checkpoints to generate checkpoints...",
        );
        while !is_zakura_checkpoints_finished.load(Ordering::SeqCst) {
            // Just keep silently checking the Zebra logs for errors,
            // so the checkpoint list can be copied from the output.
            //
            // Make sure the sync is still finished, this is logged every minute or so.
            if env::var(LOG_ZAKURAD_CHECKPOINTS).is_ok() {
                zakurad_mut.expect_stdout_line_matches(SYNC_FINISHED_REGEX)?;
            } else {
                zakurad_mut.expect_stdout_line_matches_silent(SYNC_FINISHED_REGEX)?;
            }
        }

        Ok(zakurad_mut)
    };

    // Wait until `zakura-checkpoints` has generated a full set of checkpoints.
    // Also checks `zakura-checkpoints` logs for errors.
    //
    // Checkpoints generation can take a long time, so we need to run it in parallel with `zakurad`.
    let zakura_checkpoints_mut = &mut zakura_checkpoints;
    let zakura_checkpoints_wait_fn = || -> Result<_> {
        tracing::debug!(
            ?test_type,
            "waiting for zakura_checkpoints to generate checkpoints...",
        );

        // zakura-checkpoints does not log anything when it finishes, it just prints checkpoints.
        //
        // We know that checkpoints are always less than 1000 blocks apart, but they can happen
        // anywhere in that range due to block sizes. So we ignore the last 3 digits of the height.
        let expected_final_checkpoint_prefix1 = expected_final_checkpoint_height.0 / 1000;
        // To ensure it also works on corner cases, we also consider the next possible checkpoint.
        let expected_final_checkpoint_prefix2 = expected_final_checkpoint_prefix1 + 1;

        // Mainnet and testnet checkpoints always have at least one leading zero in their hash.
        let expected_final_checkpoint =
            format!("({expected_final_checkpoint_prefix1}[0-9][0-9][0-9]|{expected_final_checkpoint_prefix2}[0-9][0-9][0-9]) 0");
        zakura_checkpoints_mut.expect_stdout_line_matches(&expected_final_checkpoint)?;

        // Write the rest of the checkpoints: there can be 0-2 more checkpoints.
        while zakura_checkpoints_mut.wait_for_stdout_line(None) {}

        // Tell the other thread that `zakura_checkpoints` has finished
        is_zakura_checkpoints_finished.store(true, Ordering::SeqCst);

        Ok(zakura_checkpoints_mut)
    };

    // Run both threads in parallel, automatically propagating any panics to this thread.
    std::thread::scope(|s| {
        // Launch the sync-waiting threads
        let zakurad_thread = s.spawn(|| {
            zakurad_wait_fn().expect("test failed while waiting for zakurad to sync");
        });

        let zakura_checkpoints_thread = s.spawn(|| {
            let zakura_checkpoints_result = zakura_checkpoints_wait_fn();

            is_zakura_checkpoints_finished.store(true, Ordering::SeqCst);

            zakura_checkpoints_result
                .expect("test failed while waiting for zakura_checkpoints to sync.");
        });

        // Mark the sync-waiting threads as finished if they fail or panic.
        // This tells the other thread that it can exit.
        //
        // TODO: use `panic::catch_unwind()` instead,
        //       when `&mut zakura_test::command::TestChild<TempDir>` is unwind-safe
        s.spawn(|| {
            let zakurad_result = zakurad_thread.join();
            zakurad_result.expect("test panicked or failed while waiting for zakurad to sync");
        });
        s.spawn(|| {
            let zakura_checkpoints_result = zakura_checkpoints_thread.join();
            is_zakura_checkpoints_finished.store(true, Ordering::SeqCst);

            zakura_checkpoints_result
                .expect("test panicked or failed while waiting for zakura_checkpoints to sync");
        });
    });

    Ok((zakura_checkpoints, zakurad))
}

/// Returns an approximate `zakurad` tip height, using JSON-RPC.
#[tracing::instrument]
pub async fn zakurad_tip_height(zakura_rpc_address: SocketAddr) -> Result<Height> {
    let client = RpcRequestClient::new(zakura_rpc_address);

    let zakurad_blockchain_info = client
        .text_from_call("getblockchaininfo", "[]".to_string())
        .await?;
    let zakurad_blockchain_info: serde_json::Value =
        serde_json::from_str(&zakurad_blockchain_info)?;

    let zakurad_tip_height = zakurad_blockchain_info["result"]["blocks"]
        .try_into_height()
        .expect("unexpected block height: invalid Height value");

    Ok(zakurad_tip_height)
}
