//! Finalized state tests.

#![allow(clippy::unwrap_in_result)]

use proptest::prelude::*;

use zakura_chain::{
    block::{Block, Height},
    parameters::Network,
    LedgerState,
};

use crate::{arbitrary::Prepare, service::check, SemanticallyVerifiedBlock};

mod prop;
mod rollback;
mod transparent;
mod vectors;

/// Generates exactly the valid-commitment block prefix a finalized-state test
/// consumes, rather than preparing the standard 104-block property-test chain.
fn valid_commitment_chain(
    ledger_strategy: BoxedStrategy<LedgerState>,
    block_count: usize,
) -> BoxedStrategy<(Vec<SemanticallyVerifiedBlock>, Network)> {
    ledger_strategy
        .prop_flat_map(move |ledger| {
            let network = ledger.network.clone();
            Block::partial_chain_strategy(
                ledger,
                block_count,
                check::utxo::transparent_coinbase_spend,
                true,
            )
            .prop_map(move |blocks| {
                let blocks = blocks.iter().cloned().map(Prepare::prepare).collect();
                (blocks, network.clone())
            })
        })
        .boxed()
}

#[test]
fn checkpoint_prune_range_retains_current_height_when_range_ends_before_it() {
    let current_height = Height(9);

    assert!(
        super::checkpoint_prune_range_retains_current_height(
            current_height,
            Some((Height(1), current_height)),
        ),
        "raw transactions are still needed when the prune range ends before the current height"
    );

    assert!(
        !super::checkpoint_prune_range_retains_current_height(
            current_height,
            Some((Height(1), Height(10))),
        ),
        "raw transactions can be skipped when the prune range covers the current height"
    );

    assert!(
        !super::checkpoint_prune_range_retains_current_height(current_height, None),
        "no checkpoint prune range means there is no archive backlog to drain"
    );
}
