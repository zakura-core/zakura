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

use std::{fs, path::PathBuf};

use abscissa_core::{Command, Runnable};
use clap::Parser;
use color_eyre::eyre::{eyre, Result, WrapErr};

use zakura_chain::parameters::Network;

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

        Ok(())
    }
}
