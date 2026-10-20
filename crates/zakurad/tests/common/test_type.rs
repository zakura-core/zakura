//! Provides TestType enum with shared code for acceptance tests

use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use indexmap::IndexSet;

use zakura_chain::parameters::Network;
use zakura_network::CacheDir;
use zakura_test::prelude::*;
use zakurad::config::ZakuradConfig;

use super::{
    config::{default_test_config, random_known_rpc_port_config},
    failure_messages::{PROCESS_FAILURE_MESSAGES, ZAKURA_FAILURE_MESSAGES},
    sync::FINISH_PARTIAL_SYNC_TIMEOUT,
};

use TestType::*;

/// The type of integration test that we're running.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum TestType {
    /// Launch with an empty Zakura state.
    LaunchWithEmptyState,

    /// Launch with a Zakura state that might or might not be empty.
    UseAnyState,

    /// Sync a cached Zakura state to the tip without RPCs.
    UpdateZebraCachedStateNoRpc,

    /// Launch with a cached Zakura state and RPCs.
    #[allow(dead_code)]
    UpdateZebraCachedStateWithRpc,
}

/// Startup timeout for tests that do not sync a cached state.
const LAUNCH_TIMEOUT: Duration = Duration::from_secs(60);

impl TestType {
    /// Does this test need a Zakura cached state?
    pub fn needs_zakura_cached_state(&self) -> bool {
        matches!(
            self,
            UpdateZebraCachedStateNoRpc | UpdateZebraCachedStateWithRpc
        )
    }

    /// Does this test need a Zakura RPC server?
    pub fn needs_zakura_rpc_server(&self) -> bool {
        matches!(self, LaunchWithEmptyState | UpdateZebraCachedStateWithRpc)
    }

    /// Returns the Zebra state path for this test, if set.
    #[allow(clippy::print_stderr)]
    pub fn zakurad_state_path<S: AsRef<str>>(&self, test_name: S) -> Option<PathBuf> {
        // Load the effective config (defaults + optional TOML + env overrides)
        let test_name = test_name.as_ref();
        let cfg = match ZakuradConfig::load(None) {
            Ok(c) => c,
            Err(_) => {
                eprintln!(
                    "skipped {test_name:?} {self:?} test, \
                     could not load Zakura configuration",
                );
                return None;
            }
        };

        // Skip if the configured state is ephemeral; otherwise use the configured cache_dir
        if cfg.state.ephemeral {
            eprintln!(
                "skipped {test_name:?} {self:?} test, \
                 configure a persistent state cache (e.g., set [state].ephemeral=false and [state].cache_dir)",
            );
            return None;
        }

        Some(cfg.state.cache_dir)
    }

    /// Returns a Zebra config for this test.
    ///
    /// `replace_cache_dir` replaces any cached or ephemeral state.
    ///
    /// Returns `None` if the test should be skipped,
    /// and `Some(Err(_))` if the config could not be created.
    pub fn zakurad_config<Str: AsRef<str>>(
        &self,
        test_name: Str,
        use_internet_connection: bool,
        replace_cache_dir: Option<&Path>,
        network: &Network,
    ) -> Option<Result<ZakuradConfig>> {
        let config = if self.needs_zakura_rpc_server() {
            // This is what we recommend our users configure.
            random_known_rpc_port_config(true, network)
        } else {
            Ok(default_test_config(network))
        };

        let mut config = match config {
            Ok(config) => config,
            Err(error) => return Some(Err(error)),
        };

        if !use_internet_connection {
            config.network.initial_mainnet_peers = IndexSet::new();
            config.network.initial_testnet_peers = IndexSet::new();
            // Avoid reusing cached peers from disk when we're supposed to be a disconnected instance
            config.network.cache_dir = CacheDir::disabled();

            // Activate the mempool immediately by default
            config.mempool.debug_enable_at_height = Some(0);
        }

        // If we have a cached state, or we don't want to be ephemeral, update the config to use it
        if replace_cache_dir.is_some() || self.needs_zakura_cached_state() {
            let zakura_state_path = replace_cache_dir
                .map(|path| path.to_owned())
                .or_else(|| self.zakurad_state_path(test_name))?;

            config.state.ephemeral = false;
            config.state.cache_dir = zakura_state_path;

            // And reset the concurrency to the default value
            config.sync.checkpoint_verify_concurrency_limit =
                zakurad::components::sync::DEFAULT_CHECKPOINT_CONCURRENCY_LIMIT;
        }

        Some(Ok(config))
    }

    /// Returns the `zakurad` timeout for this test type.
    pub fn zakurad_timeout(&self) -> Duration {
        match self {
            LaunchWithEmptyState | UseAnyState => LAUNCH_TIMEOUT,
            UpdateZebraCachedStateNoRpc | UpdateZebraCachedStateWithRpc => {
                FINISH_PARTIAL_SYNC_TIMEOUT
            }
        }
    }

    /// Returns Zebra log regexes that indicate the tests have failed,
    /// and regexes of any failures that should be ignored.
    pub fn zakurad_failure_messages(&self) -> (Vec<String>, Vec<String>) {
        let mut zakurad_failure_messages: Vec<String> = ZAKURA_FAILURE_MESSAGES
            .iter()
            .chain(PROCESS_FAILURE_MESSAGES)
            .map(ToString::to_string)
            .collect();

        if self.needs_zakura_cached_state() {
            // Fail if we need a cached Zebra state, but it's empty
            zakurad_failure_messages.push("loaded Zakura state cache .*tip.*=.*None".to_string());
        }
        if matches!(*self, LaunchWithEmptyState) {
            // Fail if we need an empty Zebra state, but it has blocks
            zakurad_failure_messages
                .push(r"loaded Zakura state cache .*tip.*=.*Height\([1-9][0-9]*\)".to_string());
        }

        let zakurad_ignore_messages = Vec::new();

        (zakurad_failure_messages, zakurad_ignore_messages)
    }
}
