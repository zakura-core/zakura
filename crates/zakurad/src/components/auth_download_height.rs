//! Authenticates a downloaded block's claimed coinbase height without chain state.
//!
//! # Security
//!
//! Download and gossip paths read the coinbase height before full validation, so a peer could
//! rewrite it to trigger their height policies. The header hash still authenticates that height:
//!
//! - The header commits to every transaction ID through the transaction Merkle root.
//! - A V1–V4 transaction ID commits to the coinbase input, which holds the height.
//! - A V5+ transaction ID commits to the expiry height but not to the coinbase input (ZIP 244).
//!   From NU5 onward, a coinbase expiry height must equal the block height (ZIP 203).
//!
//! So a body that matches its header's Merkle root, and whose V5+ coinbase expiry height equals its
//! coinbase input height, carries the height its header commits to. Any other body cannot be the
//! block its header names. Only the supplier chose that body, so the supplier is at fault.

use zakura_chain::block::{self, Block, Height};

/// Returns whether `block`'s header can authenticate its claimed coinbase `height`.
///
/// The expiry comparison costs nothing, so it always runs. The Merkle root comparison hashes every
/// transaction, so it runs only when `policy_uses_height` is set: callers set it when the claimed
/// height would make them drop or pause the block. The verifiers check the Merkle root of every
/// block they accept, so an honest block in the download window pays for that hashing only once.
pub(crate) fn height_is_unbound(block: &Block, height: Height, policy_uses_height: bool) -> bool {
    let Some(coinbase) = block.transactions.first() else {
        return true;
    };

    if coinbase.version() >= 5 && coinbase.expiry_height() != Some(height) {
        return true;
    }

    policy_uses_height && !header_commits_to_transactions(block)
}

/// Returns whether the header's transaction Merkle root matches the body's transaction IDs.
///
/// Duplicate-transaction padding (CVE-2012-2459) leaves the coinbase in place, so it cannot change
/// the height and the verifiers reject it later.
fn header_commits_to_transactions(block: &Block) -> bool {
    let merkle_root: block::merkle::Root = block.transactions.iter().map(|tx| tx.hash()).collect();
    merkle_root == block.header.merkle_root
}

/// Clones `canonical` and rewrites its coinbase input height.
///
/// For a V5+ coinbase the block hash and transaction IDs stay the same.
#[cfg(test)]
pub(crate) fn poison_coinbase_height(canonical: &Block, height: Height) -> std::sync::Arc<Block> {
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

/// Clones `canonical` and rewrites both its coinbase input height and its expiry height.
///
/// The block hash stays the same, but the coinbase transaction ID changes.
#[cfg(test)]
pub(crate) fn poison_coinbase_height_and_expiry(
    canonical: &Block,
    height: Height,
) -> std::sync::Arc<Block> {
    use std::sync::Arc;

    let mut poisoned = Arc::unwrap_or_clone(poison_coinbase_height(canonical, height));
    let coinbase = Arc::make_mut(
        poisoned
            .transactions
            .first_mut()
            .expect("test block has a coinbase transaction"),
    );
    *coinbase.expiry_height_mut() = height;

    Arc::new(poisoned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use zakura_chain::serialization::ZcashDeserializeInto;

    fn block(bytes: &[u8]) -> Arc<Block> {
        bytes
            .zcash_deserialize_into()
            .expect("test vector deserializes")
    }

    #[test]
    fn canonical_heights_are_bound() {
        for bytes in [
            zakura_test::vectors::BLOCK_MAINNET_1046400_BYTES.as_slice(),
            zakura_test::vectors::BLOCK_MAINNET_1687107_BYTES.as_slice(),
        ] {
            let block = block(bytes);
            let height = block.coinbase_height().expect("test block has a height");
            assert!(!height_is_unbound(&block, height, true));
        }
    }

    #[test]
    fn v5_input_height_rewrite_is_unbound_without_hashing() {
        let canonical = block(&zakura_test::vectors::BLOCK_MAINNET_1687107_BYTES);
        assert_eq!(canonical.transactions[0].version(), 5);

        for height in [Height(1), Height(2_000_000)] {
            let poisoned = poison_coinbase_height(&canonical, height);
            assert_eq!(poisoned.hash(), canonical.hash());
            assert!(height_is_unbound(&poisoned, height, false));
        }
    }

    #[test]
    fn v5_input_and_expiry_rewrite_fails_the_merkle_root() {
        let canonical = block(&zakura_test::vectors::BLOCK_MAINNET_1687107_BYTES);
        let height = Height(2_000_000);
        let poisoned = poison_coinbase_height_and_expiry(&canonical, height);

        assert_eq!(poisoned.hash(), canonical.hash());
        assert!(
            !height_is_unbound(&poisoned, height, false),
            "an in-window height defers the Merkle check to the verifier"
        );
        assert!(height_is_unbound(&poisoned, height, true));
    }

    #[test]
    fn v4_input_height_rewrite_fails_the_merkle_root() {
        let canonical = block(&zakura_test::vectors::BLOCK_MAINNET_1046400_BYTES);
        assert_eq!(canonical.transactions[0].version(), 4);

        let height = Height(1);
        let poisoned = poison_coinbase_height(&canonical, height);

        assert_eq!(poisoned.hash(), canonical.hash());
        assert!(!height_is_unbound(&poisoned, height, false));
        assert!(height_is_unbound(&poisoned, height, true));
    }
}
