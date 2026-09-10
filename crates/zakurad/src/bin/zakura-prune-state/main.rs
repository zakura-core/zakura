//! Standalone finalized-state pruning utility.

// This binary reports startup and pruning failures directly to the terminal before any tracing
// subscriber is installed.
#![allow(clippy::print_stderr)]

use clap::Parser;
use zakurad::commands::prune_state::PruneStateCmd;

/// Process entry point for `zakura-prune-state`.
fn main() {
    if let Err(error) = color_eyre::install() {
        eprintln!("failed to install error handler: {error}");
        std::process::exit(1);
    }

    let command = PruneStateCmd::parse();

    if let Err(error) = command.run_with_config(zakura_state::Config::default()) {
        eprintln!("failed to prune finalized state: {error}");
        std::process::exit(1);
    }
}
