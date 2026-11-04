//! Metrics for blocks that passed semantic verification.
//!
//! These describe what a block carries: its fees and how NU7 splits them
//! between the miner and the Network Sustainability Mechanism (NSM), the
//! subsidy it pays, and how many shielded and transparent operations it
//! holds. The state's tip metrics describe where the chain is; together they
//! let an operator watch NU7's fee and block-time changes on a live node.

use std::collections::HashMap;

use zakura_chain::{
    amount::{Amount, NonNegative},
    block::{BlockRef, Height},
    parameters::{subsidy, Network},
    transaction::{self, zip317},
};

/// What the verifier knows about a block once its fees have been checked.
pub(super) struct VerifiedBlock<'a> {
    /// The verified block.
    pub block: &'a BlockRef,
    /// The block's height.
    pub height: Height,
    /// The network the block was verified against.
    pub network: &'a Network,
    /// The sum of all non-coinbase transaction fees.
    pub block_transaction_fees: Amount<NonNegative>,
    /// The subsidy the block is expected to pay, including any reissuance bonus.
    pub expected_block_subsidy: Amount<NonNegative>,
    /// Each non-coinbase transaction's fee, keyed by its mined transaction ID.
    pub transaction_fees: &'a [(transaction::Hash, Amount<NonNegative>)],
    /// The block's transaction hashes, in block order.
    pub transaction_hashes: &'a [transaction::Hash],
}

/// Records the fee, subsidy and operation-count metrics for one verified block.
pub(super) fn record(verified: VerifiedBlock<'_>) {
    let VerifiedBlock {
        block,
        height,
        network,
        block_transaction_fees,
        expected_block_subsidy,
        transaction_fees,
        transaction_hashes,
    } = verified;

    let fees = u64::from(block_transaction_fees);
    let miner_share = u64::from(subsidy::miner_fee_share(
        height,
        network,
        block_transaction_fees,
    ));
    let (nsm_share, miner_share) = split_fees(fees, miner_share);
    let subsidy_zat = u64::from(expected_block_subsidy);
    let halving_subsidy = subsidy::halving_block_subsidy(height, network)
        .map(u64::from)
        .unwrap_or(0);

    metrics::gauge!("zcash.chain.block.height").set(f64::from(height.0));
    latest_and_total("zcash.chain.block.fees.zat", fees);
    latest_and_total("zcash.chain.block.fees.nsm.zat", nsm_share);
    latest_and_total("zcash.chain.block.fees.miner.zat", miner_share);
    latest_and_total("zcash.chain.block.subsidy.zat", subsidy_zat);
    latest_and_total(
        "zcash.chain.block.subsidy.reissuance.zat",
        reissuance_bonus(subsidy_zat, halving_subsidy),
    );

    let transactions: Vec<&transaction::Transaction> = block.transactions().collect();
    let mut counts = OperationCounts::default();
    for tx in &transactions {
        counts.add(tx);
    }
    latest_and_total("zcash.chain.block.orchard_actions", counts.orchard_actions);
    latest_and_total(
        "zcash.chain.block.ironwood_actions",
        counts.ironwood_actions,
    );
    latest_and_total("zcash.chain.block.sapling_ios", counts.sapling_ios);
    latest_and_total(
        "zcash.chain.block.sprout_joinsplits",
        counts.sprout_joinsplits,
    );
    latest_and_total(
        "zcash.chain.block.transparent_inputs",
        counts.transparent_inputs,
    );
    latest_and_total(
        "zcash.chain.block.transparent_outputs",
        counts.transparent_outputs,
    );
    latest_and_total("zcash.chain.block.transactions", counts.transactions);

    let index: HashMap<&transaction::Hash, usize> = transaction_hashes
        .iter()
        .enumerate()
        .map(|(index, hash)| (hash, index))
        .collect();
    for (hash, fee) in transaction_fees {
        let fee_zat = zat(u64::from(*fee));
        metrics::histogram!("zcash.chain.block.tx.fee.zat").record(fee_zat);
        let transaction = index.get(hash).and_then(|index| transactions.get(*index));
        if let Some(transaction) = transaction {
            // ZIP 317 bills at least `GRACE_ACTIONS`, so this never divides by zero.
            let actions = zip317::conventional_actions(transaction).max(1);
            metrics::histogram!("zcash.chain.block.tx.fee_per_action.zat")
                .record(fee_zat / f64::from(actions));
        }
    }
}

/// Splits a block's aggregate fees into the NSM and miner shares, given the
/// miner share the consensus rules computed.
///
/// Before NU7 the miner keeps everything, so the NSM share is zero.
fn split_fees(fees: u64, miner_share: u64) -> (u64, u64) {
    // The miner share never exceeds the aggregate fees.
    (fees.saturating_sub(miner_share), miner_share)
}

/// The ZIP 234 reissuance bonus: whatever the expected subsidy pays beyond
/// the halving schedule.
fn reissuance_bonus(expected_subsidy: u64, halving_subsidy: u64) -> u64 {
    expected_subsidy.saturating_sub(halving_subsidy)
}

/// Sets the `name` gauge to `value` and adds `value` to the `name.total` counter.
///
/// The gauge shows the latest block; the counter lets a scraper recover every
/// block's value from consecutive scrapes even when blocks arrive faster than it
/// polls.
fn latest_and_total(name: &'static str, value: u64) {
    metrics::gauge!(name).set(zat(value));
    metrics::counter!(format!("{name}.total")).increment(value);
}

/// Converts a zatoshi amount or an operation count to a gauge value.
fn zat(value: u64) -> f64 {
    // Amounts are bounded by MAX_MONEY (2.1e15) and counts by the block size
    // limit, both far below 2^53, so the conversion is exact.
    value as f64
}

/// Per-block operation counts, summed over every transaction.
#[derive(Default)]
struct OperationCounts {
    transactions: u64,
    orchard_actions: u64,
    ironwood_actions: u64,
    sapling_ios: u64,
    sprout_joinsplits: u64,
    transparent_inputs: u64,
    transparent_outputs: u64,
}

impl OperationCounts {
    fn add(&mut self, transaction: &transaction::Transaction) {
        let shielded = transaction.shielded_action_counts();
        self.transactions += 1;
        self.orchard_actions += u64::from(shielded.orchard_actions);
        self.ironwood_actions += u64::from(shielded.ironwood_actions);
        self.sapling_ios += u64::from(shielded.sapling_ios);
        self.sprout_joinsplits += u64::from(shielded.sprout_joinsplits);
        self.transparent_inputs += length(transaction.inputs().len());
        self.transparent_outputs += length(transaction.outputs().len());
    }
}

/// A collection length as a counter increment.
fn length(len: usize) -> u64 {
    u64::try_from(len).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nsm_share_is_whatever_the_miner_does_not_keep() {
        assert_eq!(split_fees(1_000, 400), (600, 400));
        assert_eq!(split_fees(1_000, 1_000), (0, 1_000));
        // A miner share above the fees cannot come from consensus; clamp rather than wrap.
        assert_eq!(split_fees(10, 20), (0, 20));
    }

    #[test]
    fn reissuance_bonus_is_the_subsidy_above_the_halving_schedule() {
        assert_eq!(reissuance_bonus(26_041_666 + 1_234, 26_041_666), 1_234);
        assert_eq!(reissuance_bonus(26_041_666, 26_041_666), 0);
        assert_eq!(reissuance_bonus(0, 26_041_666), 0);
    }
}
