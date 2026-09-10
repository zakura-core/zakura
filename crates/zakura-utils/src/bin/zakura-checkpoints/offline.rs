//! Offline release-state export from a Zakura state database.
//!
//! Reads canonical block hashes and `BlockInfo` sizes straight from a finalized
//! state database, so it needs no running node: the database is opened as a
//! read-only RocksDB secondary. The emitted checkpoints continue the
//! deterministic selection sequence started at the embedded Mainnet checkpoint
//! list. The optional frontier, completed-subtree, and historical frontier grid
//! artifacts are produced together at the last emitted checkpoint height, so one
//! run yields one coupled release state. See the "Mainnet release-state" section
//! of `docs/design/verified-commitment-trees.md`.
//!
//! Checkpoints, the final frontier, and the subtree roots come out of a pruned
//! database too. The frontier grid does not: it covers the heights below the
//! checkpoint, which a pruned database no longer holds. Generate from an archive
//! database.

// This is a CLI module: checkpoint lines go to stdout, status goes to stderr,
// and argument invariants established by `Args::validate_mode` use `expect`.
#![allow(clippy::print_stdout, clippy::print_stderr, clippy::unwrap_in_result)]

use std::{fs, io::Write, path::Path, time::Instant};

use color_eyre::eyre::{ensure, eyre, Context, Result};

use zakura_chain::{
    block::{self, Height, MAX_BLOCK_BYTES},
    common::atomic_write,
    parameters::Network,
};
use zakura_node_services::constants::{MAX_CHECKPOINT_BYTE_COUNT, MAX_CHECKPOINT_HEIGHT_GAP};

use crate::args::Args;

/// Default per-entry replay budget for the cost-weighted frontier grid.
///
/// Matches the 2 s budget measured in the historical-treestate serving design: a uniform
/// 50,000-block grid leaves a cold request that runs into minutes.
const DEFAULT_FRONTIER_GRID_TARGET_COST_MS: u64 = 2_000;

/// How often a long grid run reports progress, in entries.
const FRONTIER_GRID_PROGRESS_INTERVAL: u64 = 100;

/// One candidate block row read from the finalized state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BlockRow {
    /// The block's height.
    pub height: Height,
    /// The block's canonical hash.
    pub hash: block::Hash,
    /// The block's serialized size in bytes.
    pub size: u32,
}

/// Deterministically select checkpoints above `base_height` from contiguous
/// block rows, using the same cumulative byte-count and maximum height-gap rule
/// as the RPC path in `main.rs`.
///
/// Selection state fully resets at every selected checkpoint, so the sequence
/// produced from any previously selected checkpoint onward is identical to the
/// continuation of the original sequence: exports taken at different tips are
/// prefix-compatible (the release-state grid contract).
pub fn select_checkpoints(
    base_height: Height,
    rows: impl IntoIterator<Item = BlockRow>,
    max_height_gap: u32,
    max_byte_count: u64,
) -> Result<Vec<(Height, block::Hash)>> {
    let mut selected = Vec::new();
    let mut cumulative_bytes: u64 = 0;
    let mut last_height = base_height;
    let mut next_height = base_height
        .0
        .checked_add(1)
        .ok_or_else(|| eyre!("base height overflows the block height range"))?;

    for row in rows {
        ensure!(
            row.height.0 == next_height,
            "block rows must be contiguous: expected height {next_height}, got {}",
            row.height.0
        );
        next_height = row
            .height
            .0
            .checked_add(1)
            .ok_or_else(|| eyre!("block height overflows the block height range"))?;

        cumulative_bytes = cumulative_bytes
            .checked_add(u64::from(row.size))
            .ok_or_else(|| eyre!("cumulative checkpoint byte count overflowed"))?;
        let height_gap = row.height.0 - last_height.0;

        if cumulative_bytes >= max_byte_count || height_gap >= max_height_gap {
            selected.push((row.height, row.hash));
            cumulative_bytes = 0;
            last_height = row.height;
        }
    }

    Ok(selected)
}

/// Verify that a retained header extends the preceding canonical block.
fn validate_header_link(
    height: Height,
    actual_parent: block::Hash,
    expected_parent: block::Hash,
) -> Result<()> {
    ensure!(
        actual_parent == expected_parent,
        "retained block header at height {} links to {actual_parent}, but the preceding canonical \
         hash is {expected_parent}",
        height.0
    );

    Ok(())
}

/// Run the offline export selected by `--state-cache-dir`.
///
/// Prints checkpoint lines to stdout (optionally prefixed with the embedded
/// Mainnet list under `--full-list`) and writes the frontier, completed-subtree,
/// and frontier grid artifacts for the last emitted checkpoint when their output
/// paths are supplied. All status output goes to stderr so stdout stays a clean
/// checkpoint list.
pub fn run_offline(args: &Args) -> Result<()> {
    let state_cache_dir = args
        .state_cache_dir
        .clone()
        .expect("offline mode is only entered with --state-cache-dir");

    let network = Network::Mainnet;
    let embedded_max_height = network.checkpoint_list().max_height();
    let base_height = args.last_checkpoint.unwrap_or(embedded_max_height);

    let state_config = zakura_state::Config {
        cache_dir: state_cache_dir,
        delete_old_database: false,
        // Read-only export must opt into pruned mode or the state resume guard
        // correctly rejects a pruned publisher database as an archive
        // configuration. Archive databases open fine under a pruned config.
        storage_mode: zakura_state::StorageMode::Pruned(zakura_state::PruningConfig::default()),
        ..zakura_state::Config::default()
    };
    let (_read_state, db, _non_finalized_sender) =
        zakura_state::init_read_only(state_config, &network)
            .wrap_err("opening the Mainnet state database read-only")?;

    if let Some(checkpoint) = args.mainnet_frontier_grid_checkpoint {
        return backfill_frontier_grid(args, &db, &network, checkpoint);
    }

    let (tip_height, tip_hash) = db
        .tip()
        .ok_or_else(|| eyre!("Mainnet state database has no finalized tip"))?;
    ensure!(
        tip_height > base_height,
        "state tip {} is not above the last checkpoint {}; sync further before exporting",
        tip_height.0,
        base_height.0
    );
    eprintln!(
        "exporting checkpoints above {} from finalized tip {} ({tip_hash})",
        base_height.0, tip_height.0
    );

    // Anchor the export: the database must agree with the embedded checkpoint
    // list at the base, or the printed embedded prefix would stitch onto a
    // different chain (a corrupted or foreign state snapshot). A test-only
    // --last-checkpoint base off the checkpoint grid has no embedded hash to
    // compare.
    let mut previous_hash = db.hash(base_height).ok_or_else(|| {
        eyre!(
            "state database has no block at the base checkpoint {}",
            base_height.0
        )
    })?;
    if let Some(embedded_hash) = network.checkpoint_list().hash(base_height) {
        ensure!(
            previous_hash == embedded_hash,
            "state database hash at base checkpoint {} is {previous_hash}, but the embedded \
             checkpoint list has {embedded_hash}; refusing to export from a mismatched chain",
            base_height.0
        );
    }

    // Read every retained candidate row, cross-checking both hash indexes and
    // recomputing each hash from its retained header (headers survive pruning).
    // Each header must also extend the preceding canonical hash, so a corrupt
    // database fails loudly instead of exporting a disconnected header chain.
    let rows = ((base_height.0 + 1)..=tip_height.0).map(|raw_height| {
        let height = Height(raw_height);
        let hash = db
            .hash(height)
            .ok_or_else(|| eyre!("missing retained finalized hash at height {raw_height}"))?;
        ensure!(
            db.height(hash) == Some(height),
            "finalized hash indexes disagree at height {raw_height}"
        );
        let header = db
            .block_header(height.into())
            .ok_or_else(|| eyre!("missing retained block header at height {raw_height}"))?;
        ensure!(
            block::Hash::from(header.as_ref()) == hash,
            "hash index disagrees with the retained block header at height {raw_height}"
        );
        validate_header_link(height, header.previous_block_hash, previous_hash)?;
        previous_hash = hash;
        let info = db
            .block_info(height.into())
            .ok_or_else(|| eyre!("missing retained BlockInfo at height {raw_height}"))?;
        ensure!(
            u64::from(info.size()) <= MAX_BLOCK_BYTES && info.size() > 0,
            "invalid retained block size {} at height {raw_height}",
            info.size()
        );
        Ok(BlockRow {
            height,
            hash,
            size: info.size(),
        })
    });
    // Collecting first keeps row-read errors separate from selector errors.
    let rows: Vec<BlockRow> = rows.collect::<Result<_>>()?;

    let max_height_gap =
        u32::try_from(MAX_CHECKPOINT_HEIGHT_GAP).expect("checkpoint height gap fits in u32");
    let selected =
        select_checkpoints(base_height, rows, max_height_gap, MAX_CHECKPOINT_BYTE_COUNT)?;
    ensure!(
        !selected.is_empty(),
        "not enough finalized blocks above checkpoint {} to emit a new checkpoint",
        base_height.0
    );

    let &(last_height, last_hash) = selected
        .last()
        .expect("selection was checked to be non-empty");

    // Produce and persist every artifact before any checkpoint output. A failure must not leave a
    // caller's redirected stdout holding an advanced list without its coupled release state.
    if let (Some(frontier_path), Some(subtree_path), Some(grid_path)) = (
        &args.mainnet_frontier_output,
        &args.mainnet_subtree_output,
        &args.mainnet_frontier_grid_output,
    ) {
        write_release_treestate_artifacts(
            &db,
            &network,
            last_height,
            frontier_path,
            subtree_path,
            grid_path,
            args.mainnet_frontier_grid_input.as_deref(),
            frontier_grid_spacing(args),
        )?;
    }

    // Lock stdout once: the full list is ~14k lines and per-line locking is slow.
    let stdout = std::io::stdout();
    let mut stdout = stdout.lock();
    if args.full_list {
        for (height, hash) in network.checkpoint_list().iter_cloned() {
            writeln!(stdout, "{} {hash}", height.0)?;
        }
    }
    for (height, hash) in &selected {
        writeln!(stdout, "{} {hash}", height.0)?;
    }
    stdout.flush()?;

    eprintln!(
        "emitted {} checkpoints; last checkpoint {} ({last_hash})",
        selected.len(),
        last_height.0
    );

    Ok(())
}

/// Generate only the frontier grid, for a checkpoint the binary already ships.
///
/// The normal export pins every artifact to a checkpoint it selects above the embedded list.
/// That is right for an advancing release, and wrong for backfilling the one artifact a
/// committed release state is missing: it would advance `main-checkpoints.txt` as a side effect
/// of producing a file for a checkpoint already reviewed and merged.
fn backfill_frontier_grid(
    args: &Args,
    db: &zakura_state::ZakuraDb,
    network: &Network,
    checkpoint: Height,
) -> Result<()> {
    let grid_path = args
        .mainnet_frontier_grid_output
        .as_ref()
        .expect("backfill mode is only entered with --mainnet-frontier-grid-output");

    // Only an embedded checkpoint can be backfilled. A height off the reviewed list would
    // produce an artifact no committed release state can be coupled to.
    let embedded_hash = network.checkpoint_list().hash(checkpoint).ok_or_else(|| {
        eyre!(
            "{} is not an embedded Mainnet checkpoint; backfill targets a checkpoint this \
             binary already ships",
            checkpoint.0,
        )
    })?;

    let (tip_height, tip_hash) = db
        .tip()
        .ok_or_else(|| eyre!("Mainnet state database has no finalized tip"))?;
    ensure!(
        tip_height >= checkpoint,
        "state tip {} is below checkpoint {}; sync further before backfilling",
        tip_height.0,
        checkpoint.0,
    );

    // Same anchor check as the checkpoint export: a database that disagrees with the embedded
    // list at this height is a different chain, and its grid would be silently wrong.
    let database_hash = db
        .hash(checkpoint)
        .ok_or_else(|| eyre!("state database has no block at checkpoint {}", checkpoint.0))?;
    ensure!(
        database_hash == embedded_hash,
        "state database hash at checkpoint {} is {database_hash}, but the embedded checkpoint \
         list has {embedded_hash}; refusing to export from a mismatched chain",
        checkpoint.0,
    );

    eprintln!(
        "backfilling the frontier grid for embedded checkpoint {} from finalized tip {} \
         ({tip_hash}); no checkpoints are emitted",
        checkpoint.0, tip_height.0,
    );

    write_frontier_grid(
        db,
        network,
        checkpoint,
        grid_path,
        args.mainnet_frontier_grid_input.as_deref(),
        frontier_grid_spacing(args),
    )
}

/// Chooses the frontier grid layout from the CLI flags.
///
/// Cost-weighted at [`DEFAULT_FRONTIER_GRID_TARGET_COST_MS`] is the default: a uniform grid
/// cannot bound the worst-case cold request at a sane artifact size.
fn frontier_grid_spacing(args: &Args) -> zakura_state::GridSpacing {
    match args.frontier_grid_spacing {
        Some(blocks) => zakura_state::GridSpacing::Uniform { blocks },
        None => zakura_state::GridSpacing::Adaptive {
            budget_us: args
                .frontier_grid_target_cost_ms
                .unwrap_or(DEFAULT_FRONTIER_GRID_TARGET_COST_MS)
                .saturating_mul(1000),
        },
    }
}

/// Produce and atomically write the coupled treestate artifacts for `height`.
#[allow(clippy::too_many_arguments)]
fn write_release_treestate_artifacts(
    db: &zakura_state::ZakuraDb,
    network: &Network,
    height: Height,
    frontier_path: &Path,
    subtree_path: &Path,
    grid_path: &Path,
    grid_resume_path: Option<&Path>,
    grid_spacing: zakura_state::GridSpacing,
) -> Result<()> {
    let artifacts = zakura_state::produce_release_treestate_artifacts(db, height)
        .wrap_err("producing the Mainnet release treestate artifacts")?;
    zakura_state::validate_final_frontiers_bytes(&artifacts.final_frontiers, height)
        .wrap_err("validating the Mainnet release frontier bytes")?;
    atomic_write(frontier_path.to_path_buf(), &artifacts.final_frontiers)
        .wrap_err_with(|| format!("writing {}", frontier_path.display()))?
        .wrap_err_with(|| format!("persisting {}", frontier_path.display()))?;
    atomic_write(subtree_path.to_path_buf(), &artifacts.historical_subtrees)
        .wrap_err_with(|| format!("writing {}", subtree_path.display()))?
        .wrap_err_with(|| format!("persisting {}", subtree_path.display()))?;

    eprintln!(
        "wrote checkpoint {} artifacts: {}-byte frontier to {}, {}-byte subtree roots to {}",
        artifacts.last_checkpoint.0,
        artifacts.final_frontiers.len(),
        frontier_path.display(),
        artifacts.historical_subtrees.len(),
        subtree_path.display(),
    );
    eprintln!(
        "extended checkpoint {} roots with {} local rows; verified {} roots total",
        artifacts.previous_last_checkpoint.0,
        artifacts.added_subtree_roots,
        artifacts.verified_subtree_roots,
    );

    write_frontier_grid(
        db,
        network,
        height,
        grid_path,
        grid_resume_path,
        grid_spacing,
    )?;

    Ok(())
}

/// Produce and atomically write the historical frontier grid covering `[0, height)`.
///
/// Generated for the checkpoint the other two artifacts describe, so all three files pin one
/// release state. A consuming node refuses to start when its own fast-sync handoff is above the
/// grid's checkpoint, which is why the coupling has to hold rather than merely usually hold.
fn write_frontier_grid(
    db: &zakura_state::ZakuraDb,
    network: &Network,
    height: Height,
    grid_path: &Path,
    resume_path: Option<&Path>,
    spacing: zakura_state::GridSpacing,
) -> Result<()> {
    match spacing {
        zakura_state::GridSpacing::Adaptive { budget_us } => eprintln!(
            "generating the frontier grid below checkpoint {}, cost-weighted at {} ms per entry",
            height.0,
            budget_us / 1000,
        ),
        zakura_state::GridSpacing::Uniform { blocks } => eprintln!(
            "generating the frontier grid below checkpoint {}, uniform spacing {blocks}",
            height.0,
        ),
    }

    // Resuming carries a published grid's entries forward, so a run scans only the blocks above
    // its last entry. Every carried entry is re-checked against this database before it is
    // accepted, so the file supplies work already done, never trust.
    let resume_from = match resume_path {
        Some(path) => {
            let bytes = fs::read(path).wrap_err_with(|| format!("reading {}", path.display()))?;
            let grid = zakura_state::FrontierArtifact::decode(&bytes, network)
                .map_err(|error| eyre!("reading {}: {error}", path.display()))?;
            eprintln!(
                "resuming from {}: {} entries through height {}",
                path.display(),
                grid.entries.len(),
                grid.entries.last().map_or(0, |entry| entry.height.0),
            );
            Some(grid)
        }
        None => None,
    };

    // Time each grid step. One step is exactly the replay a serving node performs for a cold
    // request at this spacing, so the run doubles as the measurement that sizes the grid.
    let mut entries = 0u64;
    let mut previous = Instant::now();
    let export = zakura_state::export_frontier_grid_to(
        db,
        height,
        spacing,
        resume_from.as_ref(),
        |entry, blocks| {
            let step = previous.elapsed();
            previous = Instant::now();
            entries += 1;

            if entries.is_multiple_of(FRONTIER_GRID_PROGRESS_INTERVAL) {
                eprintln!(
                    "  entry {entries:>7}  height {:>9}  {blocks:>9} blocks replayed  \
                     last step {:>8.1}ms",
                    entry.0,
                    step.as_secs_f64() * 1e3,
                );
            }
        },
    )
    .map_err(|error| eyre!("producing the Mainnet historical frontier grid: {error}"))?;

    let grid_bytes = export.frontiers.encode(network);
    atomic_write(grid_path.to_path_buf(), &grid_bytes)
        .wrap_err_with(|| format!("writing {}", grid_path.display()))?
        .wrap_err_with(|| format!("persisting {}", grid_path.display()))?;

    eprintln!(
        "wrote {}-byte frontier grid to {}: {} entries below checkpoint {}, {} blocks replayed \
         in {:.1}s",
        grid_bytes.len(),
        grid_path.display(),
        export.frontiers.entries.len(),
        height.0,
        export.replayed_blocks,
        export.elapsed.as_secs_f64(),
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A distinct synthetic hash for test row `height`.
    fn test_hash(height: u32) -> block::Hash {
        let mut bytes = [0; 32];
        bytes[..4].copy_from_slice(&height.to_le_bytes());
        block::Hash(bytes)
    }

    /// Contiguous synthetic rows for `base + 1 ..= base + count`, all `size` bytes.
    fn rows(base: u32, count: u32, size: u32) -> Vec<BlockRow> {
        (base + 1..=base + count)
            .map(|height| BlockRow {
                height: Height(height),
                hash: test_hash(height),
                size,
            })
            .collect()
    }

    #[test]
    fn selects_on_height_gap() {
        // Tiny sizes never trip the byte rule, so only the gap rule fires.
        let selected = select_checkpoints(Height(100), rows(100, 10, 1), 4, u64::MAX)
            .expect("contiguous rows select");

        assert_eq!(
            selected
                .iter()
                .map(|(height, _)| height.0)
                .collect::<Vec<_>>(),
            vec![104, 108],
            "a checkpoint is emitted at every full height gap, and the short tail is dropped"
        );
        assert_eq!(selected[0].1, test_hash(104), "hashes follow their rows");
    }

    #[test]
    fn selects_on_byte_count() {
        // 3 rows of 40 bytes reach a 100-byte limit before the gap rule fires.
        let selected = select_checkpoints(Height(0), rows(0, 7, 40), 1000, 100)
            .expect("contiguous rows select");

        assert_eq!(
            selected
                .iter()
                .map(|(height, _)| height.0)
                .collect::<Vec<_>>(),
            vec![3, 6],
            "cumulative bytes reset at every selected checkpoint"
        );
    }

    #[test]
    fn selection_is_prefix_compatible_across_tips() {
        // The grid contract: an export from a shorter chain is a byte-for-byte
        // prefix of an export from a longer chain, and re-basing at any selected
        // checkpoint continues the same sequence.
        let long = rows(500, 100, 7);
        let full = select_checkpoints(Height(500), long.clone(), 10, 64).expect("select");

        for shorter_len in [10, 35, 61, 99] {
            let partial = select_checkpoints(Height(500), long[..shorter_len].to_vec(), 10, 64)
                .expect("select");
            assert_eq!(
                partial,
                full[..partial.len()],
                "selection from a shorter tip is a prefix of the longer selection"
            );
        }

        let (rebase_height, _) = full[1];
        let rebase_rows: Vec<BlockRow> = long
            .iter()
            .copied()
            .filter(|row| row.height > rebase_height)
            .collect();
        let rebased = select_checkpoints(rebase_height, rebase_rows, 10, 64).expect("select");
        assert_eq!(
            rebased,
            full[2..],
            "selection re-based at a selected checkpoint continues the sequence"
        );
    }

    #[test]
    fn short_chains_select_nothing() {
        let selected = select_checkpoints(Height(9), rows(9, 3, 1), 4, u64::MAX)
            .expect("contiguous rows select");
        assert!(
            selected.is_empty(),
            "chains shorter than the first trigger emit no checkpoints"
        );
    }

    #[test]
    fn non_contiguous_rows_are_rejected() {
        let mut gapped = rows(10, 5, 1);
        gapped.remove(2);

        let result = select_checkpoints(Height(10), gapped, 4, u64::MAX);
        assert!(result.is_err(), "a height gap in the rows is an error");

        let offset = rows(11, 3, 1);
        let result = select_checkpoints(Height(10), offset, 4, u64::MAX);
        assert!(
            result.is_err(),
            "rows must start immediately above the base height"
        );
    }
    #[test]
    fn header_links_must_extend_the_preceding_canonical_hash() {
        let expected_parent = test_hash(10);

        assert!(
            validate_header_link(Height(11), expected_parent, expected_parent).is_ok(),
            "a header linked to the preceding canonical hash is accepted"
        );
        assert!(
            validate_header_link(Height(11), test_hash(9), expected_parent).is_err(),
            "a disconnected retained header is rejected"
        );
    }
}
