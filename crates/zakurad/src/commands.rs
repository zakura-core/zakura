//! Zakurad Subcommands

use std::{path::PathBuf, sync::Arc};

use color_eyre::eyre::Report;

use zakura_chain::common::preference_dir;

use crate::config::ZakuradConfig;

pub use self::{entry_point::EntryPoint, start::StartCmd};

use self::{
    audit_historical_treestates::AuditHistoricalTreestatesCmd, generate::GenerateCmd,
    prune_state::PruneStateCmd, rollback_state::RollbackStateCmd, tip_height::TipHeightCmd,
    verify_historical_treestates::VerifyHistoricalTreestatesCmd,
};

pub mod start;

mod audit_historical_treestates;
mod entry_point;
mod generate;
pub mod prune_state;
pub mod rollback_state;
mod tip_height;
mod verify_historical_treestates;

#[cfg(test)]
mod tests;

use ZakuradCmd::*;

/// Zakura configuration filename.
pub const CONFIG_FILE: &str = "zakura.toml";

/// Legacy Zebra configuration filename.
const LEGACY_CONFIG_FILE: &str = "zebrad.toml";

/// Zakurad Subcommands
#[derive(Debug, clap::Subcommand)]
pub enum ZakuradCmd {
    /// Audit historical note commitment treestate serving in the state database
    AuditHistoricalTreestates(AuditHistoricalTreestatesCmd),

    /// Prove a historical subtree-root artifact against a note commitment frontier
    VerifyHistoricalTreestates(VerifyHistoricalTreestatesCmd),

    /// Generate a default `zakura.toml` configuration
    Generate(GenerateCmd),

    /// Prune Zebra's finalized raw transaction data on disk
    PruneState(PruneStateCmd),

    /// Roll back Zebra's finalized chain state on disk
    RollbackState(RollbackStateCmd),

    /// Start the application (default command)
    Start(StartCmd),

    /// Print the tip block height of Zebra's chain state on disk
    TipHeight(TipHeightCmd),
}

impl ZakuradCmd {
    /// Returns true if this command is a server command.
    ///
    /// Servers load extra components, and use the configured tracing filter.
    ///
    /// For example, `Start` acts as a Zcash node.
    pub(crate) fn is_server(&self) -> bool {
        // List all the commands, so new commands have to make a choice here
        match self {
            // Commands that run as a configured server
            Start(_) => true,

            // Utility commands that don't use server components
            AuditHistoricalTreestates(_)
            | Generate(_)
            | PruneState(_)
            | RollbackState(_)
            | TipHeight(_)
            | VerifyHistoricalTreestates(_) => false,
        }
    }

    /// Returns true if this command shows the Zebra intro logo and text.
    ///
    /// For example, `Start` acts as a Zcash node.
    pub(crate) fn uses_intro(&self) -> bool {
        // List all the commands, so new commands have to make a choice here
        match self {
            // Commands that need an intro
            Start(_) => true,

            // Utility commands
            AuditHistoricalTreestates(_)
            | Generate(_)
            | PruneState(_)
            | RollbackState(_)
            | TipHeight(_)
            | VerifyHistoricalTreestates(_) => false,
        }
    }

    /// Returns true if this command should ignore errors when
    /// attempting to load a config file.
    pub(crate) fn should_ignore_load_config_error(&self) -> bool {
        matches!(self, ZakuradCmd::Generate(_))
    }

    /// Returns the default log level for this command, based on the `verbose` command line flag.
    ///
    /// Some commands need to be quiet by default.
    pub(crate) fn default_tracing_filter(&self, verbose: bool) -> &'static str {
        let only_show_warnings = match self {
            // Commands that generate quiet output by default.
            // This output:
            // - is used by automated tools, or
            // - needs to be read easily.
            AuditHistoricalTreestates(_)
            | Generate(_)
            | PruneState(_)
            | RollbackState(_)
            | TipHeight(_)
            | VerifyHistoricalTreestates(_) => true,

            // Commands that generate informative logging output by default.
            Start(_) => false,
        };

        if only_show_warnings && !verbose {
            "warn"
        } else if only_show_warnings || !verbose {
            "info"
        } else {
            "debug"
        }
    }

    /// Runs this command with the loaded `config`.
    ///
    /// A command reports its own failure before returning an error, so callers only need
    /// to turn the error into an exit status.
    pub(crate) fn run(&self, config: Arc<ZakuradConfig>) -> Result<(), Report> {
        match self {
            AuditHistoricalTreestates(cmd) => cmd.run(&config),
            Generate(cmd) => cmd.run(),
            PruneState(cmd) => cmd.run(&config),
            RollbackState(cmd) => cmd.run(&config),
            Start(cmd) => cmd.run(config),
            TipHeight(cmd) => cmd.run(&config),
            VerifyHistoricalTreestates(cmd) => cmd.run(),
        }
    }

    /// Returns the config file in the preference directory, if there is one.
    ///
    /// Prefers `zakura.toml`, and falls back to a legacy `zebrad.toml` with a
    /// deprecation warning.
    pub(crate) fn config_path(&self) -> Option<PathBuf> {
        let if_exists = |f: PathBuf| if f.exists() { Some(f) } else { None };

        preference_dir().and_then(|path| {
            let config_path = path.join(CONFIG_FILE);
            if let Some(config_path) = if_exists(config_path) {
                return Some(config_path);
            }

            let legacy_config_path = path.join(LEGACY_CONFIG_FILE);
            if let Some(legacy_config_path) = if_exists(legacy_config_path) {
                tracing::warn!(
                    config_file = ?legacy_config_path,
                    "zebrad.toml is deprecated; rename this config file to zakura.toml"
                );
                return Some(legacy_config_path);
            }

            None
        })

        // Note: Changes in how configuration is loaded may need usage
        // edits in generate.rs
    }

    /// Applies this command's options to the loaded `config`, and checks the result.
    pub(crate) fn process_config(&self, config: ZakuradConfig) -> Result<ZakuradConfig, Report> {
        match self {
            ZakuradCmd::Start(cmd) => cmd.override_config(config),
            _ => Ok(config),
        }
    }
}
