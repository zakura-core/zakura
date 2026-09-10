//! `verify-historical-treestates` subcommand - proves a subtree-root artifact against a frontier.
//!
//! Subtree roots are interior nodes of a note commitment tree, so an artifact's framing, digest,
//! and record counts all pass whether its roots are right, wrong, or absent. A frontier does pin
//! them: its interior nodes are the pairwise hashes of the subtrees already complete, so folding
//! the artifact's roots must reproduce them.
//!
//! This command runs that check with no database and no network, against the frontier embedded in
//! this binary or one supplied on the command line. It exists so a bundle from the release-state
//! publisher can be checked before it is committed, rather than only after it ships.
//!
//! It optionally checks the bundle's frontier grid too. Every grid entry is root-checked by the
//! node that loads it, so nothing here has to prove the entries themselves. What a bundle can get
//! wrong without any node noticing until startup is the coupling: a grid generated for a
//! different checkpoint than the rest of the bundle makes a node whose fast-sync handoff sits
//! above the grid fail closed. So the grid is checked for framing and for agreeing with the
//! bundle it arrived in.

use std::{fs, path::PathBuf};

use abscissa_core::{Command, Runnable};
use clap::Parser;
use color_eyre::eyre::{eyre, Result, WrapErr};

use zakura_chain::{block::Height, parameters::Network};

/// Prove a historical subtree-root artifact against a note commitment frontier
#[derive(Command, Debug, Default, Parser)]
pub struct VerifyHistoricalTreestatesCmd {
    /// The network the artifact was generated for.
    #[clap(
        long,
        short,
        required = true,
        help = "the network the artifact was generated for"
    )]
    network: Network,

    /// The subtree-root artifact to check.
    #[clap(long, required = true, help = "path to the subtree-root artifact")]
    subtree_input: PathBuf,

    /// The frontier artifact to check it against.
    ///
    /// Defaults to the frontier embedded in this binary, which is the one the artifact will be
    /// checked against once it is committed.
    #[clap(
        long,
        help = "path to a frontier artifact; defaults to the one embedded in this binary"
    )]
    frontier_input: Option<PathBuf>,

    /// The historical frontier grid from the same bundle.
    ///
    /// Checked for framing and for covering the same checkpoint as the subtree artifact. Its
    /// entries are not proven here: a node re-derives and root-checks every one before use.
    #[clap(
        long,
        help = "path to a frontier grid artifact from the same release-state bundle"
    )]
    frontier_grid_input: Option<PathBuf>,
}

impl Runnable for VerifyHistoricalTreestatesCmd {
    /// `verify-historical-treestates` sub-command entrypoint.
    fn run(&self) {
        if let Err(error) = self.verify() {
            tracing::error!("Subtree-root artifact verification failed: {error:#}");
            std::process::exit(1);
        }
    }
}

impl VerifyHistoricalTreestatesCmd {
    /// Runs the check, returning an error if any root could not be proven.
    #[allow(clippy::print_stdout)]
    fn verify(&self) -> Result<()> {
        let subtrees = fs::read(&self.subtree_input)
            .wrap_err_with(|| format!("reading {}", self.subtree_input.display()))?;

        let frontier = self
            .frontier_input
            .as_ref()
            .map(|path| fs::read(path).wrap_err_with(|| format!("reading {}", path.display())))
            .transpose()?;

        let counts =
            zakura_state::verify_subtree_artifact(&self.network, &subtrees, frontier.as_deref())
                .map_err(|error| eyre!("{error}"))?;

        println!(
            "proved {} subtree roots against the {} frontier: sapling {}, orchard {}, ironwood {}",
            counts.total(),
            match &self.frontier_input {
                Some(path) => path.display().to_string(),
                None => "embedded".to_string(),
            },
            counts.sapling,
            counts.orchard,
            counts.ironwood,
        );

        if let Some(grid_path) = &self.frontier_grid_input {
            let checkpoint = zakura_state::SubtreeArtifact::decode(&subtrees, &self.network)
                .map_err(|error| eyre!("{error}"))?
                .last_checkpoint;
            self.verify_frontier_grid(grid_path, checkpoint)?;
        }

        Ok(())
    }

    /// Checks a frontier grid's framing, and that it belongs to this bundle.
    #[allow(clippy::print_stdout)]
    fn verify_frontier_grid(&self, grid_path: &PathBuf, checkpoint: Height) -> Result<()> {
        let bytes =
            fs::read(grid_path).wrap_err_with(|| format!("reading {}", grid_path.display()))?;
        let grid = zakura_state::FrontierArtifact::decode(&bytes, &self.network)
            .map_err(|error| eyre!("{error}"))?;

        if grid.last_checkpoint != checkpoint {
            return Err(eyre!(
                "{} covers checkpoint {}, but this bundle's subtree roots are for checkpoint {}",
                grid_path.display(),
                grid.last_checkpoint.0,
                checkpoint.0,
            ));
        }

        // The format orders entries but does not bound them, and an entry at or above the
        // checkpoint describes a height the grid does not claim to cover.
        if let Some(entry) = grid.entries.last() {
            if entry.height >= grid.last_checkpoint {
                return Err(eyre!(
                    "{} has an entry at height {}, at or above its own checkpoint {}",
                    grid_path.display(),
                    entry.height.0,
                    grid.last_checkpoint.0,
                ));
            }
        }

        println!(
            "frontier grid {} covers checkpoint {} with {} entries",
            grid_path.display(),
            grid.last_checkpoint.0,
            grid.entries.len(),
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use zakura_state::{FrontierArtifact, FrontierEntry};

    use super::*;

    /// A grid artifact at `checkpoint` with one entry per height in `heights`.
    fn grid_bytes(checkpoint: u32, heights: &[u32]) -> Vec<u8> {
        FrontierArtifact {
            spacing: 1,
            last_checkpoint: Height(checkpoint),
            entries: heights
                .iter()
                .map(|height| FrontierEntry {
                    height: Height(*height),
                    sapling: Arc::default(),
                    orchard: Arc::default(),
                    ironwood: Arc::default(),
                })
                .collect(),
        }
        .encode(&Network::Mainnet)
    }

    fn command_with_grid(bytes: &[u8]) -> (tempfile::TempDir, VerifyHistoricalTreestatesCmd) {
        let directory = tempfile::tempdir().expect("temporary directory is created");
        let path = directory.path().join("mainnet-frontier-grid.bin");
        std::fs::write(&path, bytes).expect("writing the grid succeeds");

        let command = VerifyHistoricalTreestatesCmd {
            network: Network::Mainnet,
            subtree_input: PathBuf::new(),
            frontier_input: None,
            frontier_grid_input: Some(path),
        };

        (directory, command)
    }

    fn check(bytes: &[u8], checkpoint: u32) -> Result<()> {
        let (_directory, command) = command_with_grid(bytes);
        let path = command
            .frontier_grid_input
            .clone()
            .expect("the fixture sets a grid path");

        command.verify_frontier_grid(&path, Height(checkpoint))
    }

    #[test]
    fn accepts_a_grid_for_the_bundle_checkpoint() {
        check(&grid_bytes(100, &[10, 20, 30]), 100).expect("a matching grid is accepted");
    }

    #[test]
    fn rejects_a_grid_for_another_checkpoint() {
        let error = check(&grid_bytes(90, &[10, 20]), 100)
            .expect_err("a grid from a different bundle is rejected");

        assert!(
            error.to_string().contains("covers checkpoint 90"),
            "{error}"
        );
    }

    #[test]
    fn rejects_an_entry_at_or_above_the_checkpoint() {
        let error = check(&grid_bytes(100, &[10, 100]), 100)
            .expect_err("an entry outside the covered range is rejected");

        assert!(
            error.to_string().contains("at or above its own checkpoint"),
            "{error}"
        );
    }

    #[test]
    fn rejects_bytes_that_are_not_a_grid() {
        let error =
            check(b"ZKVCTST1 not a frontier grid", 100).expect_err("bad framing is rejected");

        assert!(!error.to_string().is_empty(), "the parse error is reported");
    }
}
