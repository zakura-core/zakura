//! Offline checkpoint and VCT-frontier export from a quiesced Zakura state.
//!
//! Reads canonical block hashes and `BlockInfo` sizes straight from a finalized
//! state database, so it works on pruned databases and needs no running node.
//! The emitted checkpoints continue the deterministic selection sequence started
//! at the embedded Mainnet checkpoint list, and the optional frontier artifact
//! is captured at the last emitted checkpoint height. See the "Mainnet
//! release-state" section of `docs/design/verified-commitment-trees.md`.

// This is a CLI module: checkpoint lines go to stdout, status goes to stderr,
// and argument invariants established by `Args::validate_mode` use `expect`.
#![allow(clippy::print_stdout, clippy::print_stderr, clippy::unwrap_in_result)]

use std::{fs, io::Write, path::Path};

use color_eyre::eyre::{ensure, eyre, Context, Result};

use zakura_chain::{
    block::{self, Height, MAX_BLOCK_BYTES},
    parameters::Network,
};
use zakura_node_services::constants::{MAX_CHECKPOINT_BYTE_COUNT, MAX_CHECKPOINT_HEIGHT_GAP};

use crate::args::Args;

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

/// Run the offline export selected by `--state-cache-dir`.
///
/// Prints checkpoint lines to stdout (optionally prefixed with the embedded
/// Mainnet list under `--full-list`) and writes the frontier artifact for the
/// last emitted checkpoint when `--mainnet-frontier-output` is supplied. All
/// status output goes to stderr so stdout stays a clean checkpoint list.
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

    // Read every retained candidate row, cross-checking both hash indexes so a
    // corrupt or partially deleted database fails loudly instead of exporting
    // a wrong chain.
    let rows = ((base_height.0 + 1)..=tip_height.0).map(|raw_height| {
        let height = Height(raw_height);
        let hash = db
            .hash(height)
            .ok_or_else(|| eyre!("missing retained finalized hash at height {raw_height}"))?;
        ensure!(
            db.height(hash) == Some(height),
            "finalized hash indexes disagree at height {raw_height}"
        );
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

    // Produce the frontier before any checkpoint output: a frontier failure
    // (Sprout change in the window, unretained body, filesystem error) must
    // not leave a caller's redirected stdout holding an advanced checkpoint
    // list without its coupled frontier artifact.
    if let Some(frontier_path) = &args.mainnet_frontier_output {
        write_frontier(&db, last_height, frontier_path)?;
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

/// Produce, validate, and atomically write the frontier artifact for `height`.
///
/// `height` is the last emitted checkpoint, which sits below the finalized tip,
/// so this uses the settled-Sprout producer: it fails closed if any retained
/// block above `height` appended Sprout note commitments, and the next daily
/// export self-heals once the checkpoint sequence passes that block.
fn write_frontier(db: &zakura_state::ZakuraDb, height: Height, path: &Path) -> Result<()> {
    let bytes = zakura_state::produce_settled_final_frontiers_bytes(db, height)
        .wrap_err("producing the Mainnet final frontiers")?;
    zakura_state::validate_final_frontiers_bytes(&bytes, height)
        .wrap_err("validating the produced frontier bytes")?;

    let temporary_path = path.with_extension("tmp");
    fs::write(&temporary_path, &bytes)
        .wrap_err_with(|| format!("writing frontier artifact to {}", temporary_path.display()))?;
    fs::rename(&temporary_path, path)
        .wrap_err_with(|| format!("renaming frontier artifact to {}", path.display()))?;

    eprintln!(
        "wrote {}-byte frontier artifact for checkpoint {} to {}",
        bytes.len(),
        height.0,
        path.display()
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
}
