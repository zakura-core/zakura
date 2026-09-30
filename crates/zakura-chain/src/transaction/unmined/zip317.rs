//! An implementation of the [ZIP-317] fee calculations for [UnminedTx]s:
//! - [conventional fee](https://zips.z.cash/zip-0317#fee-calculation)
//! - [block production transaction weight](https://zips.z.cash/zip-0317#block-production)

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

/// The marginal fee for the ZIP-317 fee calculation, in zatoshis per logical action.
//
// TODO: allow Amount<NonNegative> in constants
const MARGINAL_FEE: u64 = 5_000;

/// Zakura's minimum relay fee in zatoshis per conventional action.
const MIN_MEMPOOL_FEE_PER_ACTION: u64 = 400;

/// Nominal per-kilobyte relay fee reported by legacy RPC responses.
///
/// The actual mempool minimum is [`MIN_MEMPOOL_FEE_PER_ACTION`] per logical
/// action, so this informational value does not represent that minimum.
pub const MIN_MEMPOOL_TX_FEE_RATE: usize = 100;

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

/// Returns the conventional fee for `transaction`, as defined by [ZIP-317].
///
/// [ZIP-317]: https://zips.z.cash/zip-0317#fee-calculation
pub fn conventional_fee(transaction: &Transaction) -> Amount<NonNegative> {
    // zcash_primitives checks for non-p2pkh inputs, but Zebra doesn't.
    // Conventional fees are only used in the standard rules for mempool eviction
    // and block production, so these implementations are compatible.
    //
    // <https://github.com/zcash/librustzcash/blob/main/zcash_primitives/src/transaction/fees/zip317.rs#L135>

    let marginal_fee: Amount<NonNegative> = MARGINAL_FEE.try_into().expect("fits in amount");

    // marginal_fee * max(logical_actions, GRACE_ACTIONS)
    let conventional_fee = marginal_fee * conventional_actions(transaction).into();

    conventional_fee.expect("conventional fee is positive and limited by serialized size limit")
}

/// Returns the number of unpaid actions for `transaction`, as defined by [ZIP-317].
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
    // Subtracting MAX_MONEY/5000 from a u32 can't go above i64::MAX.
    let unpaid_actions = i64::from(conventional_actions) - marginal_fee_weight_ratio;

    unpaid_actions.try_into().unwrap_or_default()
}

/// Returns the block production fee weight ratio for `transaction`, as defined by [ZIP-317].
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

/// Check Zakura's minimum fee before inserting a transaction into the mempool.
pub fn mempool_checks(
    transaction: &UnminedTx,
    miner_fee: Amount<NonNegative>,
) -> Result<(), Error> {
    let fee_per_action =
        Amount::<NonNegative>::try_from(MIN_MEMPOOL_FEE_PER_ACTION).expect("fee fits in amount");
    let min_fee = (fee_per_action * conventional_actions(&transaction.transaction).into())
        .expect("minimum fee is limited by the transaction size limit");

    if miner_fee < min_fee {
        return Err(Error::FeeBelowMinimumRate);
    }

    Ok(())
}

/// Errors related to mempool fee policy.
#[derive(Error, Clone, Debug, PartialEq, Eq)]
#[allow(missing_docs)]
pub enum Error {
    #[error("Transaction fee is below the minimum relay fee")]
    FeeBelowMinimumRate,
}
