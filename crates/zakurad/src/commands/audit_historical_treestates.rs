//! `audit-historical-treestates` subcommand - reports and measures historical treestate serving.
//!
//! A verified-commitment-trees fast-synced node serves historical treestates by replaying block
//! bodies forward and checking each result against the authenticated roots it already stores. This
//! command reports whether a state database has the inputs that needs, and optionally walks the
//! absent band to confirm the root check holds at every height and to measure what replay costs.
//!
//! Read-only: it opens the database as a secondary instance and never writes.

use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use abscissa_core::{Application, Command, Runnable};
use clap::Parser;
use color_eyre::eyre::{eyre, Result, WrapErr};

use zakura_chain::{block::Height, parameters::Network};
use zakura_state::{
    DerivationSample, HistoricalTreeCache, VctTreestateInventory,
    DEFAULT_MAX_HISTORICAL_TREE_REPLAY_BLOCKS,
};

use crate::prelude::APPLICATION;

/// Audit historical note commitment treestate serving in an existing state database
#[derive(Command, Debug, Default, Parser)]
pub struct AuditHistoricalTreestatesCmd {
    /// Path to Zakura's cached state.
    #[clap(long, short, help = "path to directory with the Zakura chain state")]
    cache_dir: Option<PathBuf>,

    /// The network of the chain to audit.
    #[clap(
        long,
        short,
        required = true,
        help = "the network of the chain to load"
    )]
    network: Network,

    /// Derive frontiers across the absent band, checking each against the stored root.
    ///
    /// Without this the command only reports the inventory, which is fast.
    #[clap(
        long,
        help = "walk the absent band, deriving and root-checking every sampled height"
    )]
    walk: bool,

    /// Derive every `step`th height rather than every height.
    ///
    /// A step of 1 walks contiguously, which is the strongest invariant check. Larger steps
    /// approximate a wallet's scan batch size and measure what a client actually pays.
    #[clap(long, default_value = "1", help = "height spacing for the walk")]
    step: u32,

    /// The first height to derive. Defaults to the bottom of the absent band.
    #[clap(long, help = "first height to derive")]
    from: Option<u32>,

    /// The last height to derive. Defaults to the top of the absent band.
    #[clap(long, help = "last height to derive")]
    to: Option<u32>,

    /// Start each derivation from a fresh memo, measuring cold replay rather than sequential.
    ///
    /// This is what sizes the published frontier grid: it reports what a client pays when no
    /// nearby frontier is memoized.
    #[clap(long, help = "clear the memo before each derivation")]
    cold: bool,

    /// Anchor derivations on a published frontier artifact.
    ///
    /// Without this, a cold derivation replays from genesis. With it, the walk measures what a
    /// node configured with the artifact actually pays, which is the number the grid is sized by.
    #[clap(long, help = "path to a frontier artifact to anchor derivations on")]
    frontier_artifact: Option<PathBuf>,

    /// Print one line per derivation with its measured cost and its replay inputs.
    ///
    /// Emits `SAMPLE <height> <blocks> <bytes> <commitments> <micros>`, which is what the grid's
    /// cost model is fitted against.
    #[clap(long, help = "print per-derivation cost and replay inputs")]
    print_samples: bool,

    /// Print the derived roots at each sampled height, for comparison against another node.
    ///
    /// Output is one `ROOT <height> <sapling> <orchard> <ironwood>` line per height, hex-encoded
    /// in the same display order `z_gettreestate` uses.
    #[clap(long, help = "print derived roots for cross-node comparison")]
    print_roots: bool,

    /// Check replay-derived subtree roots against the ones stored above the handoff.
    ///
    /// Subtree roots are interior nodes, so the per-height root check does not test them. Above
    /// the handoff the database stores them, which makes that band the one place a replay can be
    /// checked against ground truth.
    #[clap(
        long,
        help = "verify replay-derived subtree roots against stored rows above the handoff"
    )]
    verify_subtrees: bool,

    /// Skip the root-index and block-body scans, reporting only the cheap markers.
    ///
    /// Those scans visit every height in the band, which takes minutes on Mainnet. Skipping them
    /// is worth it when repeating a walk on a database already known to be complete; the walk
    /// still fails on the first height it cannot derive, so nothing goes unchecked.
    #[clap(long, help = "skip the full-band scans, reporting markers only")]
    no_scan: bool,
}

impl Runnable for AuditHistoricalTreestatesCmd {
    /// `audit-historical-treestates` sub-command entrypoint.
    fn run(&self) {
        if let Err(error) = self.run_with_config(APPLICATION.config().state.clone()) {
            tracing::error!("Failed to audit historical treestates: {error:#}");
            std::process::exit(1);
        }
    }
}

impl AuditHistoricalTreestatesCmd {
    /// Runs the audit using `state_config` as the base state configuration.
    #[allow(clippy::print_stdout)]
    pub fn run_with_config(&self, mut state_config: zakura_state::Config) -> Result<()> {
        if self.step == 0 {
            return Err(eyre!("--step must be at least 1"));
        }

        if let Some(cache_dir) = self.cache_dir.clone() {
            state_config.cache_dir = cache_dir;
        }

        let (_read_state, db, _non_finalized_sender) =
            zakura_state::init_read_only(state_config, &self.network)?;

        let inventory = zakura_state::vct_treestate_inventory(&db, !self.no_scan);
        print_inventory(&inventory);

        if self.verify_subtrees {
            let handoff = inventory
                .handoff_height
                .ok_or_else(|| eyre!("this database has no handoff, so there is no stored band"))?;
            let tip = inventory
                .finalized_tip
                .ok_or_else(|| eyre!("this database has no finalized tip"))?;

            println!();
            println!(
                "verifying replay-derived subtree roots against stored rows in ({}, {}]",
                handoff.0, tip.0
            );

            let outcome = zakura_state::verify_subtrees_against_stored(&db, handoff, tip)
                .map_err(|error| eyre!("{error}"))?;

            println!("  matched:    {}", outcome.matched);
            println!("  unstored:   {}", outcome.unstored);
            println!("  mismatched: {}", outcome.mismatched.len());
            for (index, pool) in &outcome.mismatched {
                println!("    {pool} subtree {}", index.0);
            }

            if !outcome.mismatched.is_empty() {
                return Err(eyre!(
                    "replay does not reproduce stored subtree roots, so generated subtree \
                     artifacts cannot be trusted"
                ));
            }
        }

        if !self.walk {
            return Ok(());
        }

        let Some((band_start, band_end)) = inventory.absent_band() else {
            return Err(eyre!(
                "this database has no absent band, so there is nothing to derive: it stores \
                 per-height trees at every height"
            ));
        };

        if inventory.can_derive() == Some(false) {
            return Err(eyre!(
                "this database is missing derivation inputs, so a walk would fail immediately; \
                 see the inventory above"
            ));
        }

        // The band is half-open, so its last covered height is `H - 1`.
        let from = Height(self.from.unwrap_or(band_start.0));
        let to = Height(self.to.unwrap_or(band_end.0 - 1));
        if from > to {
            return Err(eyre!("--from {} is above --to {}", from.0, to.0));
        }

        println!();
        println!(
            "walking [{}, {}] step {} ({} mode)",
            from.0,
            to.0,
            self.step,
            if self.cold { "cold" } else { "sequential" }
        );

        // Loading here, rather than inside the walk, keeps a bad path a startup error instead of
        // something discovered part-way through a multi-hour run.
        let artifact = match &self.frontier_artifact {
            Some(path) => {
                let bytes =
                    std::fs::read(path).wrap_err_with(|| format!("reading {}", path.display()))?;
                let artifact = zakura_state::FrontierArtifact::decode(&bytes, &self.network)
                    .map_err(|error| eyre!("{error}"))?;
                println!(
                    "anchoring on {} entries at spacing {}",
                    artifact.entries.len(),
                    artifact.spacing
                );
                Some(Arc::new(artifact))
            }
            None => None,
        };

        self.walk_band(&db, from, to, artifact)
    }

    /// Derives every sampled height in `[from, to]`, printing progress and a summary.
    #[allow(clippy::print_stdout)]
    fn walk_band(
        &self,
        db: &zakura_state::ZakuraDb,
        from: Height,
        to: Height,
        artifact: Option<Arc<zakura_state::FrontierArtifact>>,
    ) -> Result<()> {
        let heights: Vec<Height> = (from.0..=to.0)
            .step_by(self.step as usize)
            .map(Height)
            .collect();
        let total = heights.len();

        if self.print_roots {
            let cache = Mutex::new(match &artifact {
                Some(artifact) => HistoricalTreeCache::with_artifact(artifact.clone()),
                None => HistoricalTreeCache::default(),
            });
            let roots = zakura_state::derived_roots_in_display_order(
                db,
                &cache,
                heights,
                DEFAULT_MAX_HISTORICAL_TREE_REPLAY_BLOCKS,
            )
            .map_err(|(height, error)| {
                eyre!("derivation failed at height {}: {error}", height.0)
            })?;

            for (height, sapling, orchard, ironwood) in roots {
                println!("ROOT {} {sapling} {orchard} {ironwood}", height.0);
            }

            return Ok(());
        }

        let print_samples = self.print_samples;
        let mut samples: Vec<DerivationSample> = Vec::with_capacity(total);
        let mut report = |sample: &DerivationSample| {
            if print_samples {
                // The replayed range ends at this height and covers `replayed_blocks` blocks.
                let from = Height(
                    sample.height.0 + 1
                        - sample.replayed_blocks.min(u64::from(sample.height.0) + 1) as u32,
                );
                let inputs = zakura_state::replay_inputs(db, from, sample.height);
                println!(
                    "SAMPLE {} {} {} {} {}",
                    sample.height.0,
                    inputs.blocks,
                    inputs.bytes,
                    inputs.commitments,
                    sample.elapsed.as_micros()
                );
            }
            // One line per 1000 samples keeps a multi-million-height walk readable.
            if samples.len().is_multiple_of(1000) {
                println!(
                    "  {:>7}/{total}  height {:>9}  replayed {:>9} blocks in {:>10}",
                    samples.len(),
                    sample.height.0,
                    sample.replayed_blocks,
                    format_duration(sample.elapsed),
                );
            }
            samples.push(*sample);
        };

        let new_cache = || match &artifact {
            Some(artifact) => HistoricalTreeCache::with_artifact(artifact.clone()),
            None => HistoricalTreeCache::default(),
        };

        let result = if self.cold {
            // A fresh memo per height forces every derivation to replay from the bottom of the
            // band, which is the cost a client pays with no nearby frontier.
            let mut collected = Ok(Vec::new());
            for height in heights {
                let cache = Mutex::new(new_cache());
                match zakura_state::measure_derivations(
                    db,
                    &cache,
                    [height],
                    DEFAULT_MAX_HISTORICAL_TREE_REPLAY_BLOCKS,
                    &mut report,
                ) {
                    Ok(_) => {}
                    Err(error) => {
                        collected = Err(error);
                        break;
                    }
                }
            }
            collected.map(|_: Vec<DerivationSample>| ())
        } else {
            let cache = Mutex::new(new_cache());
            zakura_state::measure_derivations(
                db,
                &cache,
                heights,
                DEFAULT_MAX_HISTORICAL_TREE_REPLAY_BLOCKS,
                &mut report,
            )
            .map(|_| ())
        };

        print_walk_summary(&samples);

        result.map_err(|(height, error)| eyre!("derivation failed at height {}: {error}", height.0))
    }
}

#[allow(clippy::print_stdout)]
fn print_inventory(inventory: &VctTreestateInventory) {
    println!("historical treestate inventory:");
    println!(
        "  finalized tip:          {}",
        show(inventory.finalized_tip)
    );
    println!(
        "  upgrade height (U):     {}",
        show(inventory.upgrade_height)
    );
    println!(
        "  handoff height (H):     {}",
        show(inventory.handoff_height)
    );
    println!(
        "  lowest retained height: {}",
        inventory.lowest_retained_height.map_or_else(
            || "none (archive)".to_string(),
            |height| height.0.to_string()
        )
    );

    match inventory.absent_band() {
        Some((start, end)) => println!(
            "  absent band:            [{}, {}), {} heights",
            start.0,
            end.0,
            end.0 - start.0
        ),
        None => println!("  absent band:            none (per-height trees at every height)"),
    }

    println!(
        "  root index gap:         {}",
        inventory.root_index_gap.map_or_else(
            || "none (gap-free)".to_string(),
            |height| format!("at height {}", height.0)
        )
    );
    println!(
        "  missing block body:     {}",
        inventory.missing_block_body.map_or_else(
            || "none (all retained)".to_string(),
            |height| format!("at height {}", height.0)
        )
    );
    println!(
        "  can derive:             {}",
        match inventory.can_derive() {
            Some(true) => "yes",
            Some(false) => "no",
            None => "not checked (--no-scan)",
        }
    );
    println!(
        "  scan took:              {}",
        format_duration(inventory.scan_duration)
    );
}

#[allow(clippy::print_stdout)]
fn print_walk_summary(samples: &[DerivationSample]) {
    println!();
    println!("walk summary:");
    println!("  heights derived and root-checked: {}", samples.len());

    let replayed: u64 = samples.iter().map(|sample| sample.replayed_blocks).sum();
    let total: Duration = samples.iter().map(|sample| sample.elapsed).sum();
    println!("  blocks replayed:                  {replayed}");
    println!(
        "  total time:                       {}",
        format_duration(total)
    );

    if replayed > 0 {
        let per_block = total / u32::try_from(replayed).unwrap_or(u32::MAX);
        println!(
            "  mean per replayed block:          {}",
            format_duration(per_block)
        );
    }

    let mut per_derivation: Vec<Duration> = samples.iter().map(|sample| sample.elapsed).collect();
    per_derivation.sort_unstable();

    for (label, quantile) in [("median", 50), ("p90", 90), ("p99", 99), ("max", 100)] {
        if let Some(value) = percentile(&per_derivation, quantile) {
            println!("  per-derivation {label:<19}{}", format_duration(value));
        }
    }
}

/// Returns the `quantile`th percentile of `sorted`, or `None` if it is empty.
fn percentile(sorted: &[Duration], quantile: usize) -> Option<Duration> {
    if sorted.is_empty() {
        return None;
    }

    let index = (sorted.len() - 1) * quantile / 100;
    sorted.get(index).copied()
}

fn show(height: Option<Height>) -> String {
    height.map_or_else(|| "none".to_string(), |height| height.0.to_string())
}

fn format_duration(duration: Duration) -> String {
    if duration < Duration::from_micros(1) {
        format!("{}ns", duration.as_nanos())
    } else if duration < Duration::from_millis(1) {
        format!("{:.1}us", duration.as_secs_f64() * 1e6)
    } else if duration < Duration::from_secs(1) {
        format!("{:.1}ms", duration.as_secs_f64() * 1e3)
    } else {
        format!("{:.2}s", duration.as_secs_f64())
    }
}
