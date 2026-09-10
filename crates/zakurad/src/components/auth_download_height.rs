//! Authenticates a downloaded block's claimed coinbase height when it builds on our chain tip.
//!
//! # Security
//!
//! A V5+ coinbase height is authorizing data and is not committed by the block hash. Download and
//! gossip paths inspect it before full validation, so a peer could rewrite it to influence their
//! height policies. A block whose parent is our best tip has one authenticated height: `tip + 1`.
//! This module centralizes that check for both paths.

use zakura_chain::block::{self, Height};

/// Returns the expected height when a child of `best_tip` claims a different coinbase height.
///
/// Returns `None` without a tip, for a different parent, or when the claimed height matches.
/// A mismatch identifies an invalid body and should be attributed to its supplying peer.
pub(crate) fn tip_child_mismatch(
    previous_block_hash: block::Hash,
    block_height: Height,
    best_tip: Option<(Height, block::Hash)>,
) -> Option<Height> {
    let (tip_height, tip_hash) = best_tip?;

    if previous_block_hash != tip_hash {
        return None;
    }

    // Committed heights are at most `Height::MAX`, so this cannot saturate. Saturating arithmetic
    // keeps the comparison fail-closed if that invariant changes.
    let expected_height = Height(tip_height.0.saturating_add(1));

    (block_height != expected_height).then_some(expected_height)
}

/// Clones `canonical` and rewrites its V5+ coinbase height without changing its block hash.
#[cfg(test)]
pub(crate) fn poison_coinbase_height(
    canonical: &zakura_chain::block::Block,
    height: Height,
) -> std::sync::Arc<zakura_chain::block::Block> {
    use std::sync::Arc;
    use zakura_chain::transparent;

    let mut poisoned = canonical.clone();

    let coinbase = Arc::make_mut(
        poisoned
            .transactions
            .first_mut()
            .expect("test block has a coinbase transaction"),
    );

    match coinbase
        .inputs_mut()
        .first_mut()
        .expect("coinbase transaction has an input")
    {
        transparent::Input::Coinbase {
            height: coinbase_height,
            ..
        } => *coinbase_height = height,
        transparent::Input::PrevOut { .. } => panic!("the first input is a coinbase input"),
    }

    Arc::new(poisoned)
}

#[cfg(test)]
mod tests {
    use super::*;

    const TIP_HASH: block::Hash = block::Hash([0xAA; 32]);
    const OTHER_HASH: block::Hash = block::Hash([0xBB; 32]);

    #[test]
    fn tip_child_with_the_expected_height_is_accepted() {
        assert_eq!(
            tip_child_mismatch(TIP_HASH, Height(101), Some((Height(100), TIP_HASH))),
            None,
        );
    }

    #[test]
    fn tip_child_with_a_rewritten_low_height_is_a_mismatch() {
        assert_eq!(
            tip_child_mismatch(TIP_HASH, Height(1), Some((Height(100), TIP_HASH))),
            Some(Height(101)),
            "a height rewritten far behind the tip must be reported, \
             not left to the behind-tip policy"
        );
    }

    #[test]
    fn tip_child_with_a_rewritten_high_height_is_a_mismatch() {
        assert_eq!(
            tip_child_mismatch(TIP_HASH, Height(500_000), Some((Height(100), TIP_HASH))),
            Some(Height(101)),
        );
    }

    #[test]
    fn a_block_that_is_not_a_tip_child_is_not_checked() {
        assert_eq!(
            tip_child_mismatch(OTHER_HASH, Height(1), Some((Height(100), TIP_HASH))),
            None,
            "without a known parent the height can't be authenticated from the tip alone",
        );
    }

    #[test]
    fn no_tip_means_no_check() {
        assert_eq!(tip_child_mismatch(TIP_HASH, Height(1), None), None);
    }
}
