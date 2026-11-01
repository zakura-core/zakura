//! Standalone finalized-state rollback utility.

// This binary reports startup and rollback failures directly to the terminal before any tracing
// subscriber is installed.
#![allow(clippy::print_stderr)]

use clap::Parser;
use zakurad::commands::rollback_state::RollbackStateCmd;

#[cfg(target_arch = "x86_64")]
#[path = "../zakurad/cpu_features.rs"]
mod cpu_features;

/// Process entry point for `zakura-rollback-state`.
fn main() {
    #[cfg(target_arch = "x86_64")]
    cpu_features::exit_if_cpu_lacks_pasta_assembly_features();

    let command = RollbackStateCmd::parse();

    if let Err(error) = command.run_with_config(
        zakura_state::Config::default(),
        zakura_consensus::Config::default(),
    ) {
        eprintln!("failed to roll back finalized state: {error}");
        std::process::exit(1);
    }
}
