//! Block template transaction selection under the
//! [ZIP-317 block production](https://zips.z.cash/zip-0317#block-production) limits.
//!
//! ZIP-317 recommends weighted-random selection. Zakura instead selects
//! deterministically by ancestor-package fee per cost, which ZIP-317 permits
//! because selection is not consensus-critical. Templates still respect the
//! ZIP-317 block size, sigop and unpaid-action limits, and the ZIP 218
//! shielded limits once they are active.

use zakura_chain::{
    amount::{self, Amount},
    block::{Height, MAX_BLOCK_BYTES},
    parameters::{
        Network, NetworkUpgrade, GLOBAL_SHIELDED_BUDGET, ORCHARD_PROTOCOL_BLOCK_ACTION_LIMIT,
        SAPLING_BLOCK_IO_LIMIT, SPROUT_BLOCK_JOINSPLIT_LIMIT,
    },
    serialization::{CompactSizeMessage, TrustedPreallocate, ZcashSerialize},
    transaction::{
        self, zip317::BLOCK_UNPAID_ACTION_LIMIT, ShieldedActionCounts, VerifiedUnminedTx,
    },
    work::equihash::Solution,
};
use zakura_consensus::{error::TransactionError, MAX_BLOCK_SIGOPS};
use zakura_node_services::mempool::TransactionDependencies;

use crate::methods::types::transaction::{CoinbaseResourceUsage, TransactionTemplate};

mod indexed;

#[cfg(test)]
mod selection_tests;

#[cfg(test)]
mod tests;

#[cfg(test)]
use crate::methods::types::get_block_template::InBlockTxDependenciesDepth;

use super::MinerParams;

use indexed::IndexedSelector;

/// Used in the return type of [`select_mempool_transactions()`] for test compilations.
#[cfg(test)]
pub(crate) type SelectedMempoolTx = (InBlockTxDependenciesDepth, VerifiedUnminedTx);

/// Used in the return type of [`select_mempool_transactions()`] for non-test compilations.
#[cfg(not(test))]
pub(crate) type SelectedMempoolTx = VerifiedUnminedTx;

/// The serialized size of the block header nonce.
const BLOCK_HEADER_NONCE_BYTES: usize = 32;

/// Returns the block header and transaction-count bytes reserved by a template.
fn block_template_overhead_bytes(net: &Network) -> usize {
    let solution_bytes = Solution::for_proposal_for_network(net).zcash_serialized_size();
    let header_bytes = Solution::INPUT_LENGTH + BLOCK_HEADER_NONCE_BYTES + solution_bytes;

    // Reserve the largest CompactSize prefix possible for a valid block's
    // transaction count, so the budget does not need to change during selection.
    let max_transaction_count =
        usize::try_from(<transaction::Transaction as TrustedPreallocate>::max_allocation())
            .expect("the maximum block transaction count fits in memory");
    let transaction_count_bytes = CompactSizeMessage::try_from(max_transaction_count)
        .expect("the maximum block transaction count fits in a network message")
        .zcash_serialized_size();

    header_bytes + transaction_count_bytes
}

/// Selects mempool transactions for block production within the [ZIP-317]
/// limits, using fee-independent coinbase resource usage and the mempool.
///
/// Selection is deterministic: packages are ranked by descending fee per cost
/// of the transaction with its unselected mempool ancestors, then oldest
/// insertion time and TXID.
/// Parents always precede their children. `mempool_txs` should be in the
/// mempool's mining order, which already ranks packages this way.
///
/// The reserved maximum coinbase transaction size includes the largest valid
/// coinbase input script, so mining pools can append their tags. Coinbase size
/// and sigops do not depend on the reward amount, which lets selection avoid
/// generating a zero-fee shielded proof.
///
/// Returns selected transactions from `mempool_txs`, or an error if coinbase
/// resource usage cannot be calculated from the parent balance.
///
/// [ZIP-317]: https://zips.z.cash/zip-0317#block-production
#[allow(clippy::too_many_arguments, clippy::unwrap_in_result)]
pub fn select_mempool_transactions(
    net: &Network,
    height: Height,
    miner_params: &MinerParams,
    nsm_value_balance: Option<Amount<amount::NonNegative>>,
    mempool_txs: Vec<VerifiedUnminedTx>,
    mempool_tx_deps: TransactionDependencies,
) -> Result<Vec<SelectedMempoolTx>, TransactionError> {
    let coinbase_resources =
        TransactionTemplate::coinbase_resource_usage(net, height, miner_params, nsm_value_balance)?;
    let limits = BlockTemplateLimits::initial(net, height, coinbase_resources);

    Ok(IndexedSelector::new(mempool_txs, mempool_tx_deps, limits).select())
}

/// Tracks the remaining capacity of a block template against every limit a
/// candidate mempool transaction can exhaust: the ZIP-317 byte, sigop, and
/// unpaid-action limits, and, once ZIP 218 is active, the per-pool shielded
/// action limits and the global shielded budget.
///
/// The shielded fields start at [`u32::MAX`] while ZIP 218 is inactive, so the
/// new limits have no effect on those templates.
#[derive(Clone)]
struct BlockTemplateLimits {
    remaining_bytes: usize,
    remaining_sigops: u32,
    remaining_unpaid_actions: u32,
    remaining_orchard_actions: u32,
    remaining_ironwood_actions: u32,
    remaining_sapling_ios: u32,
    remaining_sprout_joinsplits: u32,
    remaining_shielded_cost: u32,
}

impl BlockTemplateLimits {
    /// Returns the initial limits for a block template at `height`, with the
    /// block overhead and coinbase resources already deducted from every
    /// applicable limit.
    fn initial(network: &Network, height: Height, coinbase: CoinbaseResourceUsage) -> Self {
        let shielded_limits =
            Self::remaining_shielded_limits(network, height, coinbase.shielded_action_counts);

        let max_block_bytes: usize = MAX_BLOCK_BYTES.try_into().expect("fits in memory");
        let reserved_block_bytes = block_template_overhead_bytes(network)
            .checked_add(coinbase.max_serialized_size)
            .expect("block template byte reservation fits in memory");

        Self {
            remaining_bytes: max_block_bytes
                .checked_sub(reserved_block_bytes)
                .expect("the coinbase and block overhead fit in a block"),
            remaining_sigops: MAX_BLOCK_SIGOPS
                .checked_sub(coinbase.sigops)
                .expect("coinbase sigops fit within the block limit"),
            remaining_unpaid_actions: BLOCK_UNPAID_ACTION_LIMIT,
            remaining_orchard_actions: shielded_limits.orchard_actions,
            remaining_ironwood_actions: shielded_limits.ironwood_actions,
            remaining_sapling_ios: shielded_limits.sapling_ios,
            remaining_sprout_joinsplits: shielded_limits.sprout_joinsplits,
            remaining_shielded_cost: shielded_limits.cost,
        }
    }

    /// Returns the ZIP 218 capacity left after the generated coinbase.
    fn remaining_shielded_limits(
        network: &Network,
        height: Height,
        coinbase: ShieldedActionCounts,
    ) -> RemainingShieldedLimits {
        if !NetworkUpgrade::is_nu7_active(network, height) {
            return RemainingShieldedLimits {
                orchard_actions: u32::MAX,
                ironwood_actions: u32::MAX,
                sapling_ios: u32::MAX,
                sprout_joinsplits: u32::MAX,
                cost: u32::MAX,
            };
        }

        RemainingShieldedLimits {
            orchard_actions: ORCHARD_PROTOCOL_BLOCK_ACTION_LIMIT
                .checked_sub(coinbase.orchard_actions)
                .expect("a generated coinbase satisfies the Orchard action limit"),
            ironwood_actions: ORCHARD_PROTOCOL_BLOCK_ACTION_LIMIT
                .checked_sub(coinbase.ironwood_actions)
                .expect("a generated coinbase satisfies the Ironwood action limit"),
            sapling_ios: SAPLING_BLOCK_IO_LIMIT
                .checked_sub(coinbase.sapling_ios)
                .expect("a generated coinbase satisfies the Sapling I/O limit"),
            sprout_joinsplits: SPROUT_BLOCK_JOINSPLIT_LIMIT
                .checked_sub(coinbase.sprout_joinsplits)
                .expect("a generated coinbase satisfies the Sprout JoinSplit limit"),
            cost: GLOBAL_SHIELDED_BUDGET
                .checked_sub(coinbase.cost())
                .expect("a generated coinbase satisfies the global shielded budget"),
        }
    }

    /// Adds `tx` to the block template and returns `true` if it fits within
    /// every remaining limit. Otherwise leaves `self` unchanged and returns
    /// `false`.
    ///
    /// > If the block template with this transaction included
    /// > would be within the block size limit and block sigop limit,
    /// > and block_unpaid_actions <= block_unpaid_action_limit,
    /// > add the transaction to the block template
    ///
    /// Unpaid actions are always zero for transactions that pay the conventional
    /// fee, so the unpaid action check always passes for those transactions. The
    /// sigop count is the full block-level count (legacy + P2SH), so template
    /// selection cannot produce blocks the block verifier would reject for
    /// exceeding `MAX_BLOCK_SIGOPS`. The shielded counts come from the same
    /// [`ShieldedActionCounts`]
    /// the block verifier sums, so a template cannot exceed the ZIP 218 limits
    /// either.
    fn try_add(&mut self, tx: &VerifiedUnminedTx) -> bool {
        let counts = tx.transaction.transaction().shielded_action_counts();
        let cost = counts.cost();
        let tx_block_sigops = tx.block_sigop_count();

        if tx.transaction.size() > self.remaining_bytes
            || tx_block_sigops > self.remaining_sigops
            || tx.unpaid_actions > self.remaining_unpaid_actions
            || counts.orchard_actions > self.remaining_orchard_actions
            || counts.ironwood_actions > self.remaining_ironwood_actions
            || counts.sapling_ios > self.remaining_sapling_ios
            || counts.sprout_joinsplits > self.remaining_sprout_joinsplits
            || cost > self.remaining_shielded_cost
        {
            return false;
        }

        self.remaining_bytes -= tx.transaction.size();
        self.remaining_sigops -= tx_block_sigops;
        self.remaining_unpaid_actions -= tx.unpaid_actions;
        self.remaining_orchard_actions -= counts.orchard_actions;
        self.remaining_ironwood_actions -= counts.ironwood_actions;
        self.remaining_sapling_ios -= counts.sapling_ios;
        self.remaining_sprout_joinsplits -= counts.sprout_joinsplits;
        self.remaining_shielded_cost -= cost;

        true
    }
}

/// ZIP 218 capacity remaining after the generated coinbase transaction.
struct RemainingShieldedLimits {
    orchard_actions: u32,
    ironwood_actions: u32,
    sapling_ios: u32,
    sprout_joinsplits: u32,
    cost: u32,
}
