//! Fee calculations for [UnminedTx]s using [ZIP-317] action counts.
//!
//! Zakura uses a 400-zatoshi marginal fee for mempool and block production
//! policy, rather than ZIP-317's 5,000-zatoshi conventional fee.
//!
//! [ZIP-317]: https://zips.z.cash/zip-0317

use std::cmp::max;

use thiserror::Error;

use crate::{
    amount::{Amount, NonNegative},
    block::MAX_BLOCK_BYTES,
    serialization::ZcashSerialize,
    transaction::{Transaction, UnminedTx},
};

#[cfg(test)]
mod tests;

/// Zakura's marginal fee in zatoshis per logical action.
//
// TODO: allow Amount<NonNegative> in constants
const MARGINAL_FEE: u64 = 400;

/// The number of grace logical actions allowed by the ZIP-317 fee calculation.
const GRACE_ACTIONS: u32 = 2;

/// The standard size of p2pkh inputs for the ZIP-317 fee calculation, in bytes.
const P2PKH_STANDARD_INPUT_SIZE: usize = 150;

/// The standard size of p2pkh outputs for the ZIP-317 fee calculation, in bytes.
const P2PKH_STANDARD_OUTPUT_SIZE: usize = 34;

/// The recommended weight ratio cap for ZIP-317 block production.
/// `weight_ratio_cap` in ZIP-317.
const BLOCK_PRODUCTION_WEIGHT_RATIO_CAP: f32 = 10.0;

/// The minimum fee for the block production weight ratio calculation, in zatoshis.
/// If a transaction has a lower fee, this value is used instead.
///
/// This avoids special handling for transactions with zero weight.
const MIN_BLOCK_PRODUCTION_SUBSTITUTE_FEE: i64 = 1;

/// Maximum unpaid actions in Zakura's mempool and block templates.
///
/// Zero requires each transaction to pay the full 400-zatoshi fee per action.
pub const BLOCK_UNPAID_ACTION_LIMIT: u32 = 0;

/// The legacy minimum fee per kilobyte for Zakura mempool transactions.
///
/// Based on `DEFAULT_MIN_RELAY_TX_FEE` in `zcashd`:
/// <https://github.com/zcash/zcash/blob/f512291ff20098291442e83713de89bcddc07546/src/main.h#L71-L72>
///
/// This is a `usize` to simplify transaction size-based calculation code.
pub const MIN_MEMPOOL_TX_FEE_RATE: usize = 100;

/// The cap for the legacy size-based relay fee.
///
/// At two grace actions, the 400-zatoshi marginal fee already requires 800
/// zatoshis, so the legacy size-based fee cannot exceed the per-action fee.
pub const MEMPOOL_TX_FEE_REQUIREMENT_CAP: usize = 800;

/// Returns Zakura's conventional fee for `transaction` using [ZIP-317] actions.
///
/// [ZIP-317]: https://zips.z.cash/zip-0317#fee-calculation
pub fn conventional_fee(transaction: &Transaction) -> Amount<NonNegative> {
    // The action count follows ZIP-317; Zakura's marginal fee is lower.

    let marginal_fee: Amount<NonNegative> = MARGINAL_FEE.try_into().expect("fits in amount");

    // marginal_fee * max(logical_actions, GRACE_ACTIONS)
    let conventional_fee = marginal_fee * conventional_actions(transaction).into();

    conventional_fee.expect("conventional fee is positive and limited by serialized size limit")
}

/// Returns unpaid actions for `transaction` under Zakura's marginal fee.
///
/// [ZIP-317]: https://zips.z.cash/zip-0317#block-production
pub fn unpaid_actions(transaction: &UnminedTx, miner_fee: Amount<NonNegative>) -> u32 {
    // max(logical_actions, GRACE_ACTIONS)
    let conventional_actions = conventional_actions(&transaction.transaction);

    // floor(tx.fee / marginal_fee)
    let marginal_fee_weight_ratio = miner_fee / MARGINAL_FEE;
    let marginal_fee_weight_ratio: i64 = marginal_fee_weight_ratio
        .expect("marginal fee is not zero")
        .into();

    // max(0, conventional_actions - marginal_fee_weight_ratio)
    //
    // Subtracting MAX_MONEY/MARGINAL_FEE from a u32 can't exceed i64::MAX.
    let unpaid_actions = i64::from(conventional_actions) - marginal_fee_weight_ratio;

    unpaid_actions.try_into().unwrap_or_default()
}

/// Returns Zakura's block production fee weight ratio for `transaction`.
///
/// This calculation will always return a positive, non-zero value.
///
/// [ZIP-317]: https://zips.z.cash/zip-0317#block-production
pub fn conventional_fee_weight_ratio(
    transaction: &UnminedTx,
    miner_fee: Amount<NonNegative>,
) -> f32 {
    // Check that this function will always return a positive, non-zero value.
    //
    // The maximum number of logical actions in a block is actually
    // MAX_BLOCK_BYTES / MIN_ACTION_BYTES. MIN_ACTION_BYTES is currently
    // the minimum transparent output size, but future transaction versions could change this.
    assert!(
        MIN_BLOCK_PRODUCTION_SUBSTITUTE_FEE as f32 / MAX_BLOCK_BYTES as f32 > 0.0,
        "invalid block production constants: the minimum fee ratio must not be zero"
    );

    let miner_fee = max(miner_fee.into(), MIN_BLOCK_PRODUCTION_SUBSTITUTE_FEE) as f32;

    let conventional_fee = i64::from(transaction.conventional_fee) as f32;

    let uncapped_weight = miner_fee / conventional_fee;

    uncapped_weight.min(BLOCK_PRODUCTION_WEIGHT_RATIO_CAP)
}

/// Returns the conventional actions for `transaction`, `max(logical_actions, GRACE_ACTIONS)`,
/// as defined by [ZIP-317].
///
/// [ZIP-317]: https://zips.z.cash/zip-0317#fee-calculation
pub fn conventional_actions(transaction: &Transaction) -> u32 {
    let tx_in_total_size: usize = transaction
        .inputs()
        .iter()
        .map(|input| input.zcash_serialized_size())
        .sum();

    let tx_out_total_size: usize = transaction
        .outputs()
        .iter()
        .map(|output| output.zcash_serialized_size())
        .sum();

    let n_join_split = transaction.joinsplit_count();
    let n_spends_sapling = transaction.sapling_spends_per_anchor().count();
    let n_outputs_sapling = transaction.sapling_outputs().count();
    let n_actions_orchard = transaction.orchard_actions().count();
    let n_actions_ironwood = transaction.ironwood_actions().count();

    let tx_in_logical_actions = tx_in_total_size.div_ceil(P2PKH_STANDARD_INPUT_SIZE);
    let tx_out_logical_actions = tx_out_total_size.div_ceil(P2PKH_STANDARD_OUTPUT_SIZE);

    let logical_actions = max(tx_in_logical_actions, tx_out_logical_actions)
        + 2 * n_join_split
        + max(n_spends_sapling, n_outputs_sapling)
        + n_actions_orchard
        + n_actions_ironwood;
    let logical_actions: u32 = logical_actions
        .try_into()
        .expect("transaction items are limited by serialized size limit");

    max(GRACE_ACTIONS, logical_actions)
}

/// Make ZIP-317 checks before inserting a transaction into the mempool.
pub fn mempool_checks(
    unpaid_actions: u32,
    miner_fee: Amount<NonNegative>,
    transaction_size: usize,
) -> Result<(), Error> {
    // With a zero unpaid-action limit, each transaction must pay Zakura's
    // marginal fee for every conventional action.
    if unpaid_actions > BLOCK_UNPAID_ACTION_LIMIT {
        return Err(Error::UnpaidActions);
    }

    // Preserve the legacy size-based check for relay compatibility. Its 800
    // zatoshi cap cannot exceed the minimum for two grace actions, so the
    // per-action fee is the effective mempool minimum.

    const KILOBYTE: usize = 1000;

    let max_block_size = usize::try_from(MAX_BLOCK_BYTES).map_err(|_| Error::InvalidMinFee)?;

    // Blocks, and therefore, txs are limited to 2 MB and usize is at least 4 GB, implying that we
    // can multiply MIN_MEMPOOL_TX_FEE_RATE by transaction_size without overflow.
    assert!(MIN_MEMPOOL_TX_FEE_RATE < usize::MAX / max_block_size);

    let min_fee: u64 = (MIN_MEMPOOL_TX_FEE_RATE * transaction_size / KILOBYTE)
        .clamp(MIN_MEMPOOL_TX_FEE_RATE, MEMPOOL_TX_FEE_REQUIREMENT_CAP)
        .try_into()
        .map_err(|_| Error::InvalidMinFee)?;

    let min_fee = Amount::<NonNegative>::try_from(min_fee).map_err(|_| Error::InvalidMinFee)?;

    if miner_fee < min_fee {
        return Err(Error::FeeBelowMinimumRate);
    }

    Ok(())
}

/// Errors related to ZIP-317.
#[derive(Error, Clone, Debug, PartialEq, Eq)]
#[allow(missing_docs)]
pub enum Error {
    #[error("Unpaid actions is higher than the limit")]
    UnpaidActions,

    #[error("Transaction fee is below the minimum fee rate")]
    FeeBelowMinimumRate,

    #[error("Minimum fee could not be calculated")]
    InvalidMinFee,
}
