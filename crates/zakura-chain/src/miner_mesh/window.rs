//! Which blocks count towards a gateway's weight.

use std::ops::RangeInclusive;

use crate::{
    block::Height,
    parameters::{Network, NetworkUpgrade},
};

/// How many blocks below the tip the newest counted block sits.
///
/// Reorgs deeper than a few blocks are rare, so a block this deep almost
/// always stays on the best chain and the counts change little when the tip
/// moves. When a reorg does reach a counted block, the owner of the counts
/// compares them with the new best chain and drops the displaced blocks.
///
/// Counted blocks leave the best chain in any reorg deeper than
/// `QUALIFYING_DEPTH`. The tracker drops them, except a block read from a chain
/// that was best only briefly (see zakurad's tracker). At 25-second blocks a
/// block starts counting about four minutes after it is mined.
pub const QUALIFYING_DEPTH: u32 = 10;

/// How long a block keeps counting: two days at the target block spacing.
const WINDOW_SECONDS: i64 = 2 * 24 * 60 * 60;

/// Returns the heights that count when the best chain's tip is at `tip`.
///
/// The newest is [`QUALIFYING_DEPTH`] blocks below the tip. Older blocks count
/// while the target spacing summed from them to the newest block is under two
/// days, so the range holds two days' worth of blocks: 2,304 at a 75-second
/// spacing and 6,912 at 25 seconds. Close to a spacing change it mixes both.
///
/// Returns `None` while the tip is below [`QUALIFYING_DEPTH`], when no block
/// is that deep yet.
pub(super) fn window(network: &Network, tip: Height) -> Option<RangeInclusive<Height>> {
    let newest = tip.0.checked_sub(QUALIFYING_DEPTH)?;
    let mut oldest = newest;
    let mut remaining_seconds = WINDOW_SECONDS;

    let spacings: Vec<_> = NetworkUpgrade::target_spacings(network).collect();
    for (first_height, spacing) in spacings.into_iter().rev() {
        // This spacing starts above `oldest`: the chain has not reached it, or
        // a later upgrade that starts at the same height has replaced it.
        if first_height.0 > oldest {
            continue;
        }
        // Each block from `first_height` to `oldest` follows its parent by
        // `spacing`. The genesis block has no parent to step back to.
        let first_with_parent = first_height.0.max(1);
        let blocks = oldest + 1 - first_with_parent;
        // The most steps back that keep the summed spacing under the window.
        let affordable = (remaining_seconds - 1) / spacing.num_seconds();
        let steps = i64::from(blocks).min(affordable);

        remaining_seconds -= steps * spacing.num_seconds();
        oldest -= u32::try_from(steps).expect("steps is at most blocks, which is a u32");
        if steps < i64::from(blocks) {
            break;
        }
    }

    Some(Height(oldest)..=Height(newest))
}
