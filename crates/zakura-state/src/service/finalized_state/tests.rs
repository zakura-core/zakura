//! Finalized state tests.

#![allow(clippy::unwrap_in_result)]

use proptest::prelude::*;

use zakura_chain::{
    block::{Block, Height},
    parameters::Network,
    LedgerState,
};

use crate::{arbitrary::Prepare, service::check, SemanticallyVerifiedBlock};

mod issuance_accounting;
mod max_money;
mod nsm_value_balance;
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

/// A missing local branch ID must not become peer-delivery failure evidence.
#[test]
fn missing_branch_id_preserves_local_commit_failure() {
    use crate::{error::VctCommitFailure, ValidateContextError};
    use zakura_chain::{history_tree::HistoryTreeError, parameters::NetworkUpgrade};

    let _init_guard = zakura_test::init();
    let state = super::FinalizedState::new(&crate::Config::ephemeral(), &Network::Mainnet)
        .expect("the ephemeral state opens");
    let error = ValidateContextError::HistoryTreeError(std::sync::Arc::new(
        HistoryTreeError::MissingBranchId {
            network_upgrade: NetworkUpgrade::Nu7,
        },
    ));
    for failure in [
        VctCommitFailure::CurrentRoots,
        VctCommitFailure::SuccessorBoundary,
    ] {
        let result = state.vct_reject_supplied_root(Height(1), error.clone(), failure);
        assert_eq!(result.vct_failure(), None);
        assert_eq!(result.vct_retryable_height(), None);
        assert!(matches!(
            result.inner(),
            crate::CommitBlockError::ValidateContextError(source) if source.as_ref() == &error
        ));
    }
}
