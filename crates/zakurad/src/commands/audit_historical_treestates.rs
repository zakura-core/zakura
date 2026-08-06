//! `audit-historical-treestates` subcommand - reports and measures historical treestate serving.
//!
//! A verified-commitment-trees fast-synced node serves historical treestates by replaying block
//! bodies forward and checking each result against the authenticated roots it already stores. This
//! command reports whether a state database has the inputs that needs, and optionally walks the
//! absent band to confirm the root check holds at every height and to measure what replay costs.
//!
//! It opens the primary database read-only as a secondary instance. RocksDB writes its temporary
//! secondary workspace separately and removes it when the command closes the database.

use std::{path::PathBuf, sync::Mutex, time::Duration};

use abscissa_core::{Application, Command, Runnable};
use clap::Parser;
use color_eyre::eyre::{eyre, Result};

use zakura_chain::{block::Height, parameters::Network};
use zakura_state::{
    DerivationSample, HistoricalTreeCache, PruningConfig, StorageMode, SubtreeVerification,
    VctTreestateInventory, DEFAULT_MAX_HISTORICAL_TREE_REPLAY_BLOCKS,
};

use crate::prelude::APPLICATION;

/// Bounds audit bookkeeping and catches accidentally enormous CLI ranges before any allocation.
const MAX_AUDIT_SAMPLES: u64 = 5_000_000;

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

    /// Derive every `step`th height rather than every height, always including `--to`.
    ///
    /// A step of 1 walks contiguously, which is the strongest invariant check. Larger steps
    /// approximate a wallet's scan batch size and measure what a client actually pays.
    #[clap(
        long,
        default_value = "1",
        help = "height spacing for the walk; --to is always included"
    )]
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

    /// Check replay-derived subtree roots against the ones stored above the last checkpoint.
    ///
    /// Subtree roots are interior nodes, so the per-height root check does not test them. Above
    /// the last checkpoint the database stores them, which makes that band the one place a replay
    /// can be checked against ground truth.
    #[clap(
        long,
        help = "verify replay-derived subtree roots against stored rows above the last checkpoint"
    )]
    verify_subtrees: bool,

    /// Explicitly verify that every absent-band block body is retained.
    ///
    /// This expensive preflight is only valid for an inventory-only invocation. Walk and subtree
    /// modes validate the bodies in their own replay ranges.
    #[clap(
        long,
        help = "scan every absent-band height for a retained block body (inventory only)"
    )]
    scan_block_bodies: bool,

    /// Skip the authenticated-root index scan, reporting only cheap markers.
    #[clap(long, help = "skip the full-band authenticated-root index scan")]
    no_scan: bool,
}

impl Runnable for AuditHistoricalTreestatesCmd {
    /// `audit-historical-treestates` sub-command entrypoint.
    #[allow(clippy::print_stderr)]
    fn run(&self) {
        if let Err(error) = self.run_with_config(APPLICATION.config().state.clone()) {
            eprintln!("Failed to audit historical treestates: {error:#}");
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
        if self.scan_block_bodies && (self.walk || self.verify_subtrees) {
            return Err(eyre!(
                "--scan-block-bodies is inventory-only; walk and subtree modes validate their \
                 own replay ranges"
            ));
        }
        if self.scan_block_bodies && self.no_scan {
            return Err(eyre!("--scan-block-bodies conflicts with --no-scan"));
        }

        if let Some(cache_dir) = self.cache_dir.clone() {
            state_config.cache_dir = cache_dir;
            // Read-only pruned mode can inspect both archive and pruned databases, and never
            // performs pruning. This makes a standalone path override sufficient for either.
            if matches!(state_config.storage_mode, StorageMode::Archive) {
                state_config.storage_mode = StorageMode::Pruned(PruningConfig::default());
            }
        }

        state_config
            .validate_storage_mode(&self.network)
            .map_err(|error| eyre!("{error}"))?;

        let (_read_state, db, _non_finalized_sender) =
            zakura_state::init_read_only(state_config, &self.network)?;

        let scan_root_index = !self.no_scan && (self.walk || !self.verify_subtrees);
        if self.walk && scan_root_index {
            println!(
                "root-index preflight starting; the walk will validate every body in its \
                 requested range"
            );
        } else if self.verify_subtrees {
            println!(
                "inventory full-band scans skipped; subtree replay will validate every required \
                 body above the checkpoint"
            );
        } else if self.scan_block_bodies {
            println!(
                "root-index and block-body inventory scans starting. Please wait for \
                 approximately 5–15 minutes on Mainnet"
            );
        } else if scan_root_index {
            println!(
                "root-index inventory scan starting; use --scan-block-bodies to explicitly \
                 validate full-band body retention"
            );
        }
        let inventory = zakura_state::vct_treestate_inventory_with_scans(
            &db,
            scan_root_index,
            self.scan_block_bodies,
        );
        print_inventory(&inventory);

        if self.verify_subtrees {
            let last_checkpoint = inventory.last_checkpoint.ok_or_else(|| {
                eyre!("this database has no last checkpoint, so there is no stored band")
            })?;
            let tip = inventory
                .finalized_tip
                .ok_or_else(|| eyre!("this database has no finalized tip"))?;
            validate_subtree_range(last_checkpoint, tip)?;

            println!();
            println!(
                "verifying replay-derived subtree roots against stored rows in ({}, {}]",
                last_checkpoint.0, tip.0
            );

            let outcome = zakura_state::verify_subtrees_against_stored(&db, last_checkpoint, tip)
                .map_err(|error| eyre!("{error}"))?;

            println!("  matched:    {}", outcome.matched);
            println!("  unstored:   {}", outcome.unstored);
            println!("  mismatched: {}", outcome.mismatched.len());
            for (index, pool) in &outcome.mismatched {
                println!("    {pool} subtree {}", index.0);
            }
            println!("  stored only: {}", outcome.stored_only.len());
            for (index, pool) in &outcome.stored_only {
                println!("    {pool} subtree {}", index.0);
            }

            validate_subtree_verification(&outcome)?;
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

        let (from, to) = validated_walk_range(self.from, self.to, band_start, band_end)?;

        println!();
        println!(
            "walking [{}, {}] step {} ({} mode)",
            from.0,
            to.0,
            self.step,
            if self.cold { "cold" } else { "sequential" }
        );

        self.walk_band(&db, from, to)
    }

    /// Derives every sampled height in `[from, to]`, printing progress and a summary.
    #[allow(clippy::print_stdout)]
    fn walk_band(&self, db: &zakura_state::ZakuraDb, from: Height, to: Height) -> Result<()> {
        let total = sample_count(from, to, self.step);
        if total > MAX_AUDIT_SAMPLES {
            return Err(eyre!(
                "the requested range contains {total} samples, above the {MAX_AUDIT_SAMPLES} \
                 sample limit; increase --step or narrow the range"
            ));
        }
        let total = usize::try_from(total)
            .map_err(|_| eyre!("the requested sample count does not fit this platform"))?;
        let heights = sampled_heights(from, to, self.step);
        let block_count = u64::from(to.0) - u64::from(from.0) + 1;
        let progress_interval = (total / 100).max(1);

        println!("long block read/replay starting: {block_count} blocks, {total} sampled roots");
        if !self.cold && self.step > 1 {
            println!(
                "  --step controls root sampling only; sequential replay still reads every block"
            );
        }
        println!("  progress will be reported approximately every 1%");

        if self.print_roots {
            let cache = Mutex::new(HistoricalTreeCache::default());
            for height in heights {
                let mut roots = zakura_state::derived_roots_in_display_order(
                    db,
                    &cache,
                    [height],
                    DEFAULT_MAX_HISTORICAL_TREE_REPLAY_BLOCKS,
                )
                .map_err(|(height, error)| {
                    eyre!("derivation failed at height {}: {error}", height.0)
                })?;
                let (height, sapling, orchard, ironwood) = roots
                    .pop()
                    .expect("one requested derivation produces one root tuple");
                println!("ROOT {} {sapling} {orchard} {ironwood}", height.0);
            }

            return Ok(());
        }

        let print_samples = self.print_samples;
        let mut samples: Vec<DerivationSample> = Vec::with_capacity(total);
        let mut report = |sample: &DerivationSample| {
            let completed = samples.len() + 1;
            if print_samples {
                // The replayed range ends at this height and covers `replayed_blocks` blocks.
                // Cast is safe: the `min` clamps the value to `height.0 + 1`, which fits in
                // a u32 because heights stay below `u32::MAX`.
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
            if completed == 1 || completed.is_multiple_of(progress_interval) || completed == total {
                let percent = completed * 100 / total;
                println!(
                    "  {:>7}/{total} ({percent:>3}%)  height {:>9}  replayed {:>9} blocks in {:>10}",
                    completed,
                    sample.height.0,
                    sample.replayed_blocks,
                    format_duration(sample.elapsed),
                );
            }
            samples.push(*sample);
        };

        let new_cache = HistoricalTreeCache::default;

        let result = if self.cold {
            // A fresh memo per height forces every derivation to replay from the bottom of the
            // band, which is the cost a client pays with no nearby frontier.
            let mut result = Ok(());
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
                        result = Err(error);
                        break;
                    }
                }
            }
            result
        } else {
            let cache = Mutex::new(new_cache());
            zakura_state::measure_derivations(
                db,
                &cache,
                heights,
                DEFAULT_MAX_HISTORICAL_TREE_REPLAY_BLOCKS,
                &mut report,
            )
        };

        print_walk_summary(&samples);

        result.map_err(|(height, error)| eyre!("derivation failed at height {}: {error}", height.0))
    }
}

/// Validates explicit bounds and applies the committed absent-band defaults.
fn validated_walk_range(
    from: Option<u32>,
    to: Option<u32>,
    band_start: Height,
    band_end: Height,
) -> Result<(Height, Height)> {
    let from = Height(from.unwrap_or(band_start.0));
    let to = Height(to.unwrap_or(band_end.0 - 1));

    if from > Height::MAX || to > Height::MAX {
        return Err(eyre!(
            "walk bounds must not exceed the maximum block height {}",
            Height::MAX.0
        ));
    }
    if from > to {
        return Err(eyre!("--from {} is above --to {}", from.0, to.0));
    }
    if from < band_start || to >= band_end {
        return Err(eyre!(
            "walk range [{}, {}] is outside the committed absent band [{}, {})",
            from.0,
            to.0,
            band_start.0,
            band_end.0
        ));
    }

    Ok((from, to))
}

/// Rejects an empty or reversed subtree replay range before reporting that verification started.
fn validate_subtree_range(last_checkpoint: Height, finalized_tip: Height) -> Result<()> {
    if finalized_tip <= last_checkpoint {
        return Err(eyre!(
            "cannot verify stored subtrees: finalized tip {} must be above last checkpoint {}; \
             fast sync may not have reached the stored subtree band yet",
            finalized_tip.0,
            last_checkpoint.0,
        ));
    }

    Ok(())
}

/// Returns the number of heights sampled in `[from, to]`, including both endpoints.
fn sample_count(from: Height, to: Height, step: u32) -> u64 {
    let span = u64::from(to.0 - from.0);
    span / u64::from(step) + 1 + u64::from(!span.is_multiple_of(u64::from(step)))
}

/// Iterates over heights spaced by `step` in `[from, to]`, always including both endpoints.
fn sampled_heights(from: Height, to: Height, step: u32) -> impl Iterator<Item = Height> + Clone {
    let needs_endpoint = !(to.0 - from.0).is_multiple_of(step);
    let step =
        usize::try_from(step).expect("u32 values fit in usize on Zakura's supported targets");

    (from.0..=to.0)
        .step_by(step)
        .map(Height)
        .chain(needs_endpoint.then_some(to))
}

/// Requires at least one comparison and rejects incomplete or contradictory ground truth.
///
/// Every replay completion in the audited range is above the handoff, where the database is
/// expected to store subtree rows, and every stored row in that range must correspond to a replay
/// completion. Missing, mismatched, and stored-only rows all prevent verification.
fn validate_subtree_verification(outcome: &SubtreeVerification) -> Result<()> {
    if outcome.matched > 0
        && outcome.unstored == 0
        && outcome.mismatched.is_empty()
        && outcome.stored_only.is_empty()
    {
        Ok(())
    } else {
        Err(eyre!(
            "replay could not be verified against stored subtree rows: {} matched, {} missing, {} \
             mismatched, and {} stored-only; at least one match with no discrepancies is required",
            outcome.matched,
            outcome.unstored,
            outcome.mismatched.len(),
            outcome.stored_only.len(),
        ))
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
        "  last checkpoint (H):    {}",
        show(inventory.last_checkpoint)
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
        if inventory.root_index_scanned {
            inventory.root_index_gap.map_or_else(
                || "none (gap-free)".to_string(),
                |height| format!("at height {}", height.0),
            )
        } else {
            "not checked".to_string()
        }
    );
    println!(
        "  malformed root row:     {}",
        if inventory.root_index_scanned {
            inventory.malformed_root_row.map_or_else(
                || "none".to_string(),
                |height| format!("at height {} (corrupt input)", height.0),
            )
        } else {
            "not checked".to_string()
        }
    );
    println!(
        "  missing block body:     {}",
        if inventory.block_bodies_scanned {
            inventory.missing_block_body.map_or_else(
                || "none (all retained)".to_string(),
                |height| format!("at height {}", height.0),
            )
        } else {
            "not scanned (use --scan-block-bodies for full inventory)".to_string()
        }
    );
    println!(
        "  missing anchor:         {}",
        inventory.missing_anchor.map_or_else(
            || "none (all pools present)".to_string(),
            |height| format!("at height {}", height.0)
        )
    );
    println!(
        "  can derive:             {}",
        match inventory.can_derive() {
            Some(true) => "yes",
            Some(false) => "no",
            None => "not fully preflighted",
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

#[cfg(test)]
mod tests {
    use super::{
        sample_count, sampled_heights, validate_subtree_range, validate_subtree_verification,
        validated_walk_range,
    };
    use zakura_chain::{block::Height, subtree::NoteCommitmentSubtreeIndex};
    use zakura_state::SubtreeVerification;

    #[test]
    fn sampled_heights_always_include_to() {
        assert_eq!(
            sampled_heights(Height(100), Height(110), 6).collect::<Vec<_>>(),
            [Height(100), Height(106), Height(110)]
        );
        assert_eq!(
            sampled_heights(Height(100), Height(112), 6).collect::<Vec<_>>(),
            [Height(100), Height(106), Height(112)]
        );
        assert_eq!(
            sampled_heights(Height(100), Height(100), 6).collect::<Vec<_>>(),
            [Height(100)]
        );
        assert_eq!(sample_count(Height(100), Height(110), 6), 3);
        assert_eq!(sample_count(Height(100), Height(112), 6), 3);
        assert_eq!(sample_count(Height(100), Height(100), 6), 1);
        assert_eq!(
            sample_count(Height::MIN, Height(u32::MAX), 1),
            u64::from(u32::MAX) + 1
        );
    }

    #[test]
    fn walk_range_must_stay_within_committed_absent_band() {
        let band_start = Height(100);
        let band_end = Height(200);

        assert_eq!(
            validated_walk_range(None, None, band_start, band_end).unwrap(),
            (Height(100), Height(199))
        );
        assert!(validated_walk_range(Some(99), None, band_start, band_end).is_err());
        assert!(validated_walk_range(None, Some(200), band_start, band_end).is_err());
        assert!(validated_walk_range(Some(Height::MAX.0 + 1), None, band_start, band_end).is_err());
    }

    #[test]
    fn subtree_range_must_extend_above_last_checkpoint() {
        assert!(validate_subtree_range(Height(100), Height(101)).is_ok());
        assert!(validate_subtree_range(Height(100), Height(100)).is_err());
        assert!(validate_subtree_range(Height(100), Height(99)).is_err());
    }

    #[test]
    fn subtree_verification_requires_a_match_and_every_stored_row() {
        assert!(
            validate_subtree_verification(&SubtreeVerification::default()).is_err(),
            "an audit that compared no subtree rows must not pass"
        );

        let matched = SubtreeVerification {
            matched: 1,
            ..Default::default()
        };
        assert!(validate_subtree_verification(&matched).is_ok());

        let missing = SubtreeVerification {
            matched: 1,
            unstored: 1,
            ..Default::default()
        };
        assert!(
            validate_subtree_verification(&missing).is_err(),
            "a replay completion without its expected stored row must fail the audit"
        );

        let mismatched = SubtreeVerification {
            matched: 1,
            mismatched: vec![(NoteCommitmentSubtreeIndex(0), "sapling")],
            ..Default::default()
        };
        assert!(
            validate_subtree_verification(&mismatched).is_err(),
            "a replay completion that contradicts its stored row must fail the audit"
        );

        let stored_only = SubtreeVerification {
            matched: 1,
            stored_only: vec![(NoteCommitmentSubtreeIndex(1), "orchard")],
            ..Default::default()
        };
        assert!(
            validate_subtree_verification(&stored_only).is_err(),
            "a stored row without a replay completion must fail the audit"
        );
    }
}
