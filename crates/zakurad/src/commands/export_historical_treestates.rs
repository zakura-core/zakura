//! `export-historical-treestates` subcommand - generates the historical frontier grid.
//!
//! One read-only replay across the absent band emits the frontier grid a serving node anchors
//! on. Every entry is checked against the authenticated roots the database already holds, so
//! generation fails rather than publishing an entry that does not match. That check is also what
//! a consuming node re-runs before anchoring, which is why the grid needs no trust of its own.
//!
//! Completed subtree roots are not produced here: they ship embedded in the binary and are
//! refreshed by the release-state pipeline, which does not need historical block bodies.

use std::{path::PathBuf, time::Instant};

use abscissa_core::{Application, Command, Runnable};
use clap::Parser;
use color_eyre::eyre::{eyre, Result};

use zakura_chain::{common::atomic_write, parameters::Network};

use crate::prelude::APPLICATION;

/// Default per-entry replay budget for the cost-weighted grid.
///
/// Matches the 2 s budget measured in the historical-treestate serving design: a uniform
/// 50,000-block grid leaves a cold request that runs into minutes.
const DEFAULT_TARGET_COST_MS: u64 = 2_000;

/// Generate the historical frontier grid from an existing state database
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

    /// Uniform height spacing of the frontier grid, in blocks.
    ///
    /// Not recommended. Replay cost varies by more than an order of magnitude across Mainnet, so a
    /// uniform grid cannot bound the worst-case cold request at a sane size. Prefer the default
    /// cost-weighted grid.
    #[clap(
        long,
        conflicts_with = "target_cost_ms",
        help = "uniform grid spacing in blocks (not recommended; prefer --target-cost-ms)"
    )]
    spacing: Option<u32>,

    /// Per-entry replay budget in milliseconds, producing a cost-weighted grid.
    ///
    /// Defaults to 2000 ms when `--spacing` is omitted. Places entries densely where blocks are
    /// expensive to replay and sparsely where they are cheap, so it bounds the *worst* cold
    /// request rather than the average. The estimate is a deterministic function of the chain, so
    /// generator runs stay byte-identical.
    #[clap(
        long,
        conflicts_with = "spacing",
        help = "per-entry replay budget in ms (default: 2000; cost-weighted grid)"
    )]
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
        if let Some(cache_dir) = self.cache_dir.clone() {
            state_config.cache_dir = cache_dir;
        }

        let (_read_state, db, _non_finalized_sender) =
            zakura_state::init_read_only(state_config, &self.network)?;

        let spacing = self.grid_spacing();
        match spacing {
            zakura_state::GridSpacing::Adaptive { budget_us } => {
                println!(
                    "generating with a cost-weighted grid, {} ms per entry",
                    budget_us / 1000
                );
            }
            zakura_state::GridSpacing::Uniform { blocks } => {
                println!("generating with uniform grid spacing {blocks}");
            }
        }

        // Time each grid step. One step is exactly the replay a serving node performs for a cold
        // request at this spacing, so the run doubles as the measurement that sizes the grid.
        let mut entries = 0u64;
        let mut previous = Instant::now();
        let export = zakura_state::export_frontier_grid(&db, spacing, |height, blocks| {
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
        println!("blocks replayed:   {}", export.replayed_blocks);
        println!("elapsed:           {:.1}s", export.elapsed.as_secs_f64());

        // A consumer must never observe a partially written artifact, and a crash mid-write must
        // leave any previous one intact.
        let frontier_bytes = export.frontiers.encode(&self.network);
        atomic_write(self.frontier_output.clone(), &frontier_bytes)
            .map_err(|error| eyre!("writing {}: {error}", self.frontier_output.display()))?
            .map_err(|error| eyre!("persisting {}: {error}", self.frontier_output.display()))?;

        println!();
        println!(
            "wrote {} ({} bytes)",
            self.frontier_output.display(),
            frontier_bytes.len(),
        );

        Ok(())
    }

    /// Chooses the grid layout from the CLI flags.
    ///
    /// Adaptive at [`DEFAULT_TARGET_COST_MS`] is the default: a uniform grid cannot bound the
    /// worst-case cold request at a sane size.
    fn grid_spacing(&self) -> zakura_state::GridSpacing {
        match self.spacing {
            Some(blocks) => zakura_state::GridSpacing::Uniform { blocks },
            None => zakura_state::GridSpacing::Adaptive {
                budget_us: self
                    .target_cost_ms
                    .unwrap_or(DEFAULT_TARGET_COST_MS)
                    .saturating_mul(1000),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    fn parse_export(args: &[&str]) -> ExportHistoricalTreestatesCmd {
        let mut full = vec![
            "export-historical-treestates",
            "--network",
            "mainnet",
            "--frontier-output",
            "out.bin",
        ];
        full.extend(args);
        ExportHistoricalTreestatesCmd::try_parse_from(full).expect("args should parse")
    }

    #[test]
    fn default_grid_is_cost_weighted_at_two_seconds() {
        let cmd = parse_export(&[]);
        assert_eq!(
            cmd.grid_spacing(),
            zakura_state::GridSpacing::Adaptive {
                budget_us: 2_000_000
            }
        );
    }

    #[test]
    fn target_cost_ms_selects_adaptive() {
        let cmd = parse_export(&["--target-cost-ms", "1500"]);
        assert_eq!(
            cmd.grid_spacing(),
            zakura_state::GridSpacing::Adaptive {
                budget_us: 1_500_000
            }
        );
    }

    #[test]
    fn spacing_selects_uniform() {
        let cmd = parse_export(&["--spacing", "50000"]);
        assert_eq!(
            cmd.grid_spacing(),
            zakura_state::GridSpacing::Uniform { blocks: 50_000 }
        );
    }

    #[test]
    fn spacing_conflicts_with_target_cost_ms() {
        let err = ExportHistoricalTreestatesCmd::try_parse_from([
            "export-historical-treestates",
            "--network",
            "mainnet",
            "--frontier-output",
            "out.bin",
            "--spacing",
            "50000",
            "--target-cost-ms",
            "2000",
        ])
        .expect_err("uniform and cost-weighted flags cannot be combined");

        let rendered = err.to_string();
        assert!(rendered.contains("cannot be used with"), "{rendered}");
    }

    #[test]
    fn help_says_uniform_is_not_recommended() {
        let long = ExportHistoricalTreestatesCmd::try_parse_from([
            "export-historical-treestates",
            "--help",
        ])
        .expect_err("help exits")
        .to_string();
        assert!(long.contains("Not recommended"), "{long}");
        assert!(long.contains("Defaults to 2000 ms"), "{long}");

        let short =
            ExportHistoricalTreestatesCmd::try_parse_from(["export-historical-treestates", "-h"])
                .expect_err("help exits")
                .to_string();
        assert!(short.contains("not recommended"), "{short}");
        assert!(short.contains("default: 2000"), "{short}");
    }
}
