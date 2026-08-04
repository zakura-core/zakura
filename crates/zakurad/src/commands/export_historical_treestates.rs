//! `export-historical-treestates` subcommand - generates the subtree-root artifact.
//!
//! One read-only replay across the absent band emits the completed subtree roots described in
//! `docs/design/historical-treestate-serving.md` §4.6. The replay is checked against the
//! authenticated roots the database already holds, so generation fails rather than publishing
//! roots that do not match.

use std::{fs, path::PathBuf};

use abscissa_core::{Application, Command, Runnable};
use clap::Parser;
use color_eyre::eyre::{eyre, Result, WrapErr};

use zakura_chain::parameters::Network;

use crate::prelude::APPLICATION;

/// Generate the historical subtree-root artifact from an existing state database
#[derive(Command, Debug, Default, Parser)]
pub struct ExportHistoricalTreestatesCmd {
    /// Path to Zakura's cached state.
    #[clap(long, short, help = "path to directory with the Zakura chain state")]
    cache_dir: Option<PathBuf>,

    /// The network of the chain to export.
    #[clap(
        long,
        short,
        required = true,
        help = "the network of the chain to load"
    )]
    network: Network,

    /// Where to write the subtree-root artifact.
    #[clap(
        long,
        required = true,
        help = "output path for the subtree-root artifact"
    )]
    subtree_output: PathBuf,
}

impl Runnable for ExportHistoricalTreestatesCmd {
    /// `export-historical-treestates` sub-command entrypoint.
    fn run(&self) {
        if let Err(error) = self.run_with_config(APPLICATION.config().state.clone()) {
            tracing::error!("Failed to export historical treestates: {error:#}");
            std::process::exit(1);
        }
    }
}

impl ExportHistoricalTreestatesCmd {
    /// Runs generation using `state_config` as the base state configuration.
    #[allow(clippy::print_stdout)]
    pub fn run_with_config(&self, mut state_config: zakura_state::Config) -> Result<()> {
        if let Some(cache_dir) = self.cache_dir.clone() {
            state_config.cache_dir = cache_dir;
        }

        let (_read_state, db, _non_finalized_sender) =
            zakura_state::init_read_only(state_config, &self.network)?;

        let export = zakura_state::export_subtree_artifact(&db, |height, blocks| {
            println!("  reached height {:>9} after {blocks:>9} blocks", height.0);
        })
        .map_err(|error| eyre!("{error}"))?;

        println!();
        println!(
            "subtree roots:   sapling {}, orchard {}, ironwood {}",
            export.subtrees.sapling.len(),
            export.subtrees.orchard.len(),
            export.subtrees.ironwood.len(),
        );
        println!("blocks replayed: {}", export.replayed_blocks);
        println!("elapsed:         {:.1}s", export.elapsed.as_secs_f64());

        let bytes = export.subtrees.encode(&self.network);
        write_atomically(&self.subtree_output, &bytes)?;

        println!();
        println!(
            "wrote {} ({} bytes)",
            self.subtree_output.display(),
            bytes.len()
        );

        Ok(())
    }
}

/// Writes `bytes` to `path` via a temporary file and a rename.
///
/// A consumer must never observe a partially written artifact, and a crash mid-write must leave
/// any previous one intact.
fn write_atomically(path: &PathBuf, bytes: &[u8]) -> Result<()> {
    let temporary = path.with_extension("tmp");

    fs::write(&temporary, bytes).wrap_err_with(|| format!("writing {}", temporary.display()))?;
    fs::rename(&temporary, path)
        .wrap_err_with(|| format!("renaming {} to {}", temporary.display(), path.display()))?;

    Ok(())
}
