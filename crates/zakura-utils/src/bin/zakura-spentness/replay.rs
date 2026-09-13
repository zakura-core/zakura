//! Advance a separate ordinary archive state to exactly H.
//!
//! The source must have validated the retained chain. This replay does not
//! repeat signatures, proof verification, or all contextual consensus checks.

use std::{fs, path::Path};

use color_eyre::eyre::{ensure, eyre, Result};
use zakura_chain::block::{self, Height};
use zakura_state::FinalizedState;

use super::{canonical_block, exact_boundary, open, state_config, NETWORK};

/// Log replay progress every this many blocks.
const PROGRESS_INTERVAL: u32 = 10_000;

/// Replay retained source blocks into `destination` until its tip is H/hash.
///
/// A destination that already holds a prefix of the source chain resumes after its tip.
/// The source is never modified.
pub(super) fn replay(
    source: &Path,
    destination: &Path,
    height: u32,
    hash: block::Hash,
) -> Result<()> {
    fs::create_dir_all(destination)?;
    ensure!(
        fs::canonicalize(source)? != fs::canonicalize(destination)?,
        "replay destination must differ from source"
    );
    let source = open(source)?;
    ensure!(
        source.hash(Height(height)) == Some(hash),
        "source does not contain the selected H/hash"
    );

    let mut state = FinalizedState::new(&state_config(destination), &NETWORK)?;
    let start = match state.db.tip() {
        None => 0,
        Some((tip, tip_hash)) => {
            ensure!(
                tip.0 <= height && source.hash(tip) == Some(tip_hash),
                "replay state has an incompatible boundary; choose a fresh destination"
            );
            tip.0
                .checked_add(1)
                .ok_or_else(|| eyre!("replay height overflow"))?
        }
    };

    let mut trees = None;
    for h in start..=height {
        let block = canonical_block(&source, Height(h))?;
        let (_, next_trees) =
            state.commit_finalized_direct(block.into(), trees, None, "spentness archive replay")?;
        trees = Some(next_trees);
        if h.is_multiple_of(PROGRESS_INTERVAL) {
            eprintln!("replayed {h}/{height}");
        }
    }
    exact_boundary(&state.db, height, hash)?;
    Ok(())
}
