//! `export-historical-treestates` subcommand - generates the historical-treestate artifacts.
//!
//! One read-only replay across the absent band emits both artifacts described in
//! `docs/design/historical-treestate-serving.md`: the frontier grid (§4.1) and the completed
//! subtree roots (§4.6). Every grid entry is checked against the authenticated roots the database
//! already holds, so generation fails rather than publishing an entry that does not match.

use std::{fs, path::PathBuf, time::Instant};

use abscissa_core::{Application, Command, Runnable};
use clap::Parser;
use color_eyre::eyre::{eyre, Result, WrapErr};

use zakura_chain::parameters::Network;

use crate::prelude::APPLICATION;

/// Generate the historical-treestate artifacts from an existing state database
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

    /// Where to write the frontier artifact.
    #[clap(long, required = true, help = "output path for the frontier artifact")]
    frontier_output: PathBuf,

    /// Where to write the subtree-root artifact.
    #[clap(
        long,
        required = true,
        help = "output path for the subtree-root artifact"
    )]
    subtree_output: PathBuf,

    /// Uniform height spacing of the frontier grid, in blocks.
    ///
    /// Ignored when `--target-cost-ms` is given. A uniform grid leaves a long tail, because replay
    /// cost varies by more than an order of magnitude across the chain.
    #[clap(long, default_value = "50000", help = "uniform grid spacing in blocks")]
    spacing: u32,

    /// Per-entry replay budget in milliseconds, producing a cost-weighted grid.
    ///
    /// Places entries densely where blocks are expensive to replay and sparsely where they are
    /// cheap, so it bounds the *worst* cold request rather than the average. The estimate is a
    /// deterministic function of the chain, so generator runs stay byte-identical.
    #[clap(long, help = "per-entry replay budget in ms (cost-weighted grid)")]
    target_cost_ms: Option<u64>,
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
        if self.frontier_output == self.subtree_output {
            return Err(eyre!(
                "--frontier-output and --subtree-output must be different paths"
            ));
        }

        if let Some(cache_dir) = self.cache_dir.clone() {
            state_config.cache_dir = cache_dir;
        }

        let (_read_state, db, _non_finalized_sender) =
            zakura_state::init_read_only(state_config, &self.network)?;

        let spacing = match self.target_cost_ms {
            Some(budget_ms) => {
                println!("generating with a cost-weighted grid, {budget_ms} ms per entry");
                zakura_state::GridSpacing::Adaptive {
                    budget_us: budget_ms.saturating_mul(1000),
                }
            }
            None => {
                println!("generating with uniform grid spacing {}", self.spacing);
                zakura_state::GridSpacing::Uniform {
                    blocks: self.spacing,
                }
            }
        };

        // Time each grid step. One step is exactly the replay a serving node performs for a cold
        // request at this spacing, so the run doubles as the measurement that sizes the grid.
        let mut entries = 0u64;
        let mut previous = Instant::now();
        let export = zakura_state::export_treestate_artifacts(&db, spacing, |height, blocks| {
            let step = previous.elapsed();
            previous = Instant::now();
            entries += 1;

            if entries.is_multiple_of(100) {
                println!(
                    "  entry {entries:>7}  height {:>9}  {blocks:>9} blocks replayed  \
                         last step {:>8.1}ms",
                    height.0,
                    step.as_secs_f64() * 1e3,
                );
            }
        })
        .map_err(|error| eyre!("{error}"))?;

        println!();
        println!("frontier entries:  {}", export.frontiers.entries.len());
        println!(
            "subtree roots:     sapling {}, orchard {}, ironwood {}",
            export.subtrees.sapling.len(),
            export.subtrees.orchard.len(),
            export.subtrees.ironwood.len(),
        );
        println!("blocks replayed:   {}", export.replayed_blocks);
        println!("elapsed:           {:.1}s", export.elapsed.as_secs_f64());

        // Write both artifacts atomically, and the frontier first: a half-written pair is worse
        // than none, and the subtree artifact is the one that carries trust.
        let frontier_bytes = export.frontiers.encode(&self.network);
        let subtree_bytes = export.subtrees.encode(&self.network);

        write_atomically(&self.frontier_output, &frontier_bytes)?;
        write_atomically(&self.subtree_output, &subtree_bytes)?;

        println!();
        println!(
            "wrote {} ({} bytes) and {} ({} bytes)",
            self.frontier_output.display(),
            frontier_bytes.len(),
            self.subtree_output.display(),
            subtree_bytes.len(),
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
