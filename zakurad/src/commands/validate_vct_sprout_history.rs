//! `validate-vct-sprout-history` subcommand - audits repaired historical Sprout anchors.

use std::path::PathBuf;

use abscissa_core::{Application, Command, Runnable};
use clap::Parser;
use color_eyre::eyre::Result;

use zakura_chain::parameters::Network;
use zakura_state::VctSproutHistoryValidationSummary;

use crate::prelude::APPLICATION;

/// Validate repaired VCT Sprout history in an existing state database
#[derive(Command, Debug, Default, Parser)]
pub struct ValidateVctSproutHistoryCmd {
    /// Path to Zakura's cached state.
    #[clap(long, short, help = "path to directory with the Zakura chain state")]
    cache_dir: Option<PathBuf>,

    /// The network of the chain to validate.
    #[clap(
        long,
        short,
        required = true,
        help = "the network of the chain to load"
    )]
    network: Network,
}

impl Runnable for ValidateVctSproutHistoryCmd {
    /// `validate-vct-sprout-history` sub-command entrypoint.
    fn run(&self) {
        if let Err(error) = self.run_with_config(APPLICATION.config().state.clone()) {
            tracing::error!("Failed to validate VCT Sprout history: {error:#}");
            std::process::exit(1);
        }
    }
}

impl ValidateVctSproutHistoryCmd {
    /// Runs validation using `state_config` as the base state configuration.
    #[allow(clippy::print_stdout)]
    pub fn run_with_config(&self, mut state_config: zakura_state::Config) -> Result<()> {
        if let Some(cache_dir) = self.cache_dir.clone() {
            state_config.cache_dir = cache_dir;
        }

        let summary = zakura_state::validate_vct_sprout_history(state_config, &self.network)?;
        print_summary(&summary);

        Ok(())
    }
}

#[allow(clippy::print_stdout)]
fn print_summary(summary: &VctSproutHistoryValidationSummary) {
    println!("VCT Sprout history is valid:");
    println!("  finalized tip: {}", summary.finalized_tip.0);
    println!(
        "  sprout root at finalized tip height: {}",
        hex::encode(
            summary
                .sprout_root_at_finalized_tip
                .bytes_in_display_order()
        )
    );
    println!("  VCT marker: {}", summary.vct_marker.0);
    println!("  artifact handoff: {}", summary.artifact_handoff.0);
    println!("  checked Sprout anchors: {}", summary.checked_anchor_count);
}
