//! `tip-height` subcommand - prints the block height of Zebra's persisted chain state.
//!
//! Prints the chain tip height stored in Zebra's state. This is useful for developers to inspect
//! Zebra state directories.

use std::path::PathBuf;

use clap::Parser;
use eyre::{eyre, Result};

use zakura_chain::{
    block::{self},
    parameters::Network,
};

use zakura_state::ReadStateService;

use crate::config::ZakuradConfig;

/// Print the tip block height of Zebra's chain state on disk
#[derive(Debug, Default, Parser)]
pub struct TipHeightCmd {
    /// Path to Zebra's cached state.
    #[clap(long, short, help = "path to directory with the Zakura chain state")]
    cache_dir: Option<PathBuf>,

    /// The network to obtain the chain tip.
    #[clap(long, short, help = "the network of the chain to load")]
    network: Network,
}

impl TipHeightCmd {
    /// `tip-height` sub-command entrypoint.
    ///
    /// Reads the chain tip height from a cache directory with Zebra's state.
    /// A missing tip is logged, and is not a command failure.
    #[allow(clippy::print_stdout)]
    pub(crate) fn run(&self, config: &ZakuradConfig) -> Result<()> {
        match self.load_tip_height(config) {
            Ok(height) => println!("{}", height.0),
            Err(error) => tracing::error!("Failed to read chain tip height from state: {error}"),
        }

        Ok(())
    }

    /// Load the chain tip height from the state cache directory.
    fn load_tip_height(&self, config: &ZakuradConfig) -> Result<block::Height> {
        self.load_read_state(config)?
            .best_tip()
            .map(|(height, _hash)| height)
            .ok_or_else(|| eyre!("State directory doesn't have a chain tip block"))
    }

    /// Starts a state service using the `cache_dir` and `network` from the provided arguments.
    fn load_read_state(&self, config: &ZakuradConfig) -> Result<ReadStateService> {
        let mut config = config.mirrored_state_config();

        if let Some(cache_dir) = self.cache_dir.clone() {
            config.cache_dir = cache_dir;
        }

        Ok(zakura_state::init_read_only(config, &self.network)?.0)
    }
}
