//! The issuance and fee amounts that a ZIP 248 v7 coinbase transaction states.

use crate::{
    amount::{Amount, NegativeAllowed, NonNegative},
    block::Height,
    parameters::Network,
};

use super::{lockbox_deposit, miner_fee_share, SubsidyError};

/// What a [ZIP 248] v7 coinbase transaction states about its block's issuance and fees.
///
/// Block production builds the coinbase bundle from these amounts, and block validation
/// checks the coinbase bundle against them, so both derive them here.
///
/// [ZIP 248]: https://zips.z.cash/zip-0248
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct V7CoinbaseIssuance {
    /// The block subsidy (`blockSubsidy`).
    pub block_subsidy: Amount<NonNegative>,
    /// The part of the block subsidy that funding streams deposit into the lockbox
    /// (`lockboxValue`).
    pub lockbox_value: Amount<NonNegative>,
    /// The fees paid by the block's other transactions, before ZIP 235 removes a part of
    /// them; the coinbase fee bundle collects this amount.
    pub fees: Amount<NonNegative>,
    /// `NSMFeeContribution(height)`: the part of `fees` that ZIP 235 removes from
    /// circulation.
    pub nsm_fee_contribution: Amount<NonNegative>,
}

impl V7CoinbaseIssuance {
    /// Returns the amounts for the block at `height` with `block_subsidy`, whose other
    /// transactions pay `fees` in total, depositing the lockbox funding stream.
    pub fn new(
        height: Height,
        network: &Network,
        block_subsidy: Amount<NonNegative>,
        fees: Amount<NonNegative>,
    ) -> Result<Self, SubsidyError> {
        let lockbox_value = lockbox_deposit(height, network, block_subsidy)?;
        Self::with_lockbox_deposit(height, network, block_subsidy, lockbox_value, fees)
    }

    /// Returns the amounts like [`Self::new`], for the [`lockbox_deposit`] that the caller
    /// has already derived.
    pub fn with_lockbox_deposit(
        height: Height,
        network: &Network,
        block_subsidy: Amount<NonNegative>,
        lockbox_value: Amount<NonNegative>,
        fees: Amount<NonNegative>,
    ) -> Result<Self, SubsidyError> {
        let nsm_fee_contribution =
            (fees - miner_fee_share(height, network, fees)).map_err(|_| SubsidyError::Underflow)?;
        Ok(Self {
            block_subsidy,
            lockbox_value,
            fees,
            nsm_fee_contribution,
        })
    }

    /// The coinbase bundle's value pool delta: the block subsidy less the lockbox deposit
    /// and the ZIP 235 fee contribution. It can be negative.
    pub fn coinbase_delta(&self) -> Result<Amount<NegativeAllowed>, SubsidyError> {
        let negative_allowed = |amount: Amount<NonNegative>| amount.constrain::<NegativeAllowed>();
        ((negative_allowed(self.block_subsidy)? - negative_allowed(self.lockbox_value)?)
            .and_then(|remainder| remainder - negative_allowed(self.nsm_fee_contribution)?))
        .map_err(|_| SubsidyError::Underflow)
    }
}

#[cfg(test)]
mod tests;
