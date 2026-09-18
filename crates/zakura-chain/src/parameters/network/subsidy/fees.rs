//! Allocation of aggregate block fees between the miner and NSM.

use crate::{
    amount::{Amount, NonNegative},
    block::Height,
    parameters::{Network, NetworkUpgrade},
};

/// Returns the fees the coinbase must claim, excluding the block subsidy.
///
/// With the `nu7` feature enabled, NU7 contributes `floor(6 * transaction_fees / 10)`
/// to NSM and leaves the remainder to the miner. Before NU7, or without that feature,
/// all fees go to the miner. Contributions start at NU7 even before reissuance begins.
///
/// `transaction_fees` must be the sum of all non-coinbase fees in the block. Rounding
/// each transaction separately would underfund NSM. The aggregate is rounded in the
/// miner's favor, as specified by the [NU7 deployment draft].
///
/// [NU7 deployment draft]: https://github.com/zcash/zips/blob/32f447759aba83acfb20aab0757b68147643de22/zips/draft-valargroup-deploy-nu7.md#L120-L149
pub fn miner_fee_share(
    height: Height,
    network: &Network,
    transaction_fees: Amount<NonNegative>,
) -> Amount<NonNegative> {
    if !cfg!(feature = "nu7") || NetworkUpgrade::current(network, height) < NetworkUpgrade::Nu7 {
        return transaction_fees;
    }

    // Widen before multiplying: the intermediate can exceed MAX_MONEY even though
    // both resulting shares are valid amounts.
    let nsm_contribution = i128::from(i64::from(transaction_fees))
        .checked_mul(6)
        .expect("six times an i64 amount fits in i128")
        / 10;
    let nsm_contribution = Amount::try_from(nsm_contribution)
        .expect("sixty percent of a nonnegative amount is a valid amount");

    (transaction_fees - nsm_contribution)
        .expect("the NSM contribution cannot exceed the aggregate fees")
}

#[cfg(test)]
mod tests;
