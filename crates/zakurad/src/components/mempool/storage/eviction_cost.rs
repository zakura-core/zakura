//! The fee per unit of cost that a transaction must beat to evict another transaction.

use std::cmp::Ordering;

use zakura_chain::transaction::{zip317::MARGINAL_FEE, MEMPOOL_TRANSACTION_COST_THRESHOLD};

/// The fee per unit of cost that a transaction or package pays for its mempool space.
///
/// The mempool evicts the package with the lowest eviction cost first. A newcomer must pay
/// [`EvictionCost::exceeds_by_increment`] over every package it evicts.
///
/// The value is the rational `fee / cost`. Comparisons cross-multiply in `u128`, so they are
/// exact.
#[derive(Copy, Clone, Debug)]
pub struct EvictionCost {
    /// The fee in zatoshis.
    fee: u64,

    /// The ZIP-401 cost, which is never zero.
    cost: u64,
}

impl EvictionCost {
    /// Returns the eviction cost for `fee` zatoshis paid over `cost` units of mempool cost.
    pub fn new(fee: u64, cost: u64) -> Self {
        assert!(
            cost > 0,
            "ZIP-401 cost is at least MEMPOOL_TRANSACTION_COST_THRESHOLD"
        );

        Self { fee, cost }
    }

    /// Returns the eviction cost of `self` combined with `other`.
    ///
    /// The mempool combines each transaction once per package. Mempool transactions never
    /// spend the same funds, so a package's fees stay below `MAX_MONEY`, and its cost stays
    /// below the mempool cost limit.
    pub fn combine(self, other: Self) -> Self {
        Self {
            fee: self.fee.saturating_add(other.fee),
            cost: self.cost.saturating_add(other.cost),
        }
    }

    /// Returns the eviction cost of `self` without `other`, which `self` combined earlier.
    pub fn without(self, other: Self) -> Self {
        debug_assert!(
            self.fee >= other.fee && self.cost > other.cost,
            "a package includes the transactions removed from it, and its own transaction"
        );

        Self {
            fee: self.fee.saturating_sub(other.fee),
            cost: self.cost.saturating_sub(other.cost),
        }
    }

    /// Returns the ZIP-401 cost.
    pub fn cost(self) -> u64 {
        self.cost
    }

    /// Returns `true` if `self` is at least `victim` plus the eviction cost increment.
    ///
    /// The increment is one [`MARGINAL_FEE`] per [`MEMPOOL_TRANSACTION_COST_THRESHOLD`] of cost.
    /// Each eviction therefore raises the price of the next one, so an attacker who churns the
    /// mempool pays more with every round.
    pub fn exceeds_by_increment(self, victim: Self) -> bool {
        let threshold = u128::from(MEMPOOL_TRANSACTION_COST_THRESHOLD);

        // self.fee / self.cost >= victim.fee / victim.cost + MARGINAL_FEE / threshold
        let lhs = u128::from(self.fee)
            .saturating_mul(u128::from(victim.cost))
            .saturating_mul(threshold);
        let rhs = u128::from(victim.fee)
            .saturating_mul(u128::from(self.cost))
            .saturating_mul(threshold)
            .saturating_add(
                u128::from(MARGINAL_FEE)
                    .saturating_mul(u128::from(self.cost))
                    .saturating_mul(u128::from(victim.cost)),
            );

        lhs >= rhs
    }

    /// Returns the eviction cost in zatoshis per [`MEMPOOL_TRANSACTION_COST_THRESHOLD`] of cost,
    /// for metrics.
    pub fn zat_per_threshold_cost(self) -> f64 {
        // Metrics tolerate the precision loss of converting to `f64`.
        self.fee as f64 * MEMPOOL_TRANSACTION_COST_THRESHOLD as f64 / self.cost as f64
    }
}

impl PartialEq for EvictionCost {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for EvictionCost {}

impl PartialOrd for EvictionCost {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for EvictionCost {
    fn cmp(&self, other: &Self) -> Ordering {
        // A u64 times a u64 always fits in a u128.
        (u128::from(self.fee) * u128::from(other.cost))
            .cmp(&(u128::from(other.fee) * u128::from(self.cost)))
    }
}

#[cfg(test)]
mod tests {
    use zakura_chain::amount::MAX_MONEY;

    use super::*;

    #[test]
    fn orders_by_fee_per_cost() {
        assert!(EvictionCost::new(1, 2) < EvictionCost::new(1, 1));
        assert_eq!(EvictionCost::new(1, 2), EvictionCost::new(2, 4));
        assert!(EvictionCost::new(0, 10_000) < EvictionCost::new(1, u64::MAX));
    }

    #[test]
    fn increment_is_one_marginal_fee_per_threshold_cost() {
        let victim = EvictionCost::new(10_000, 10_000);

        let at_increment = EvictionCost::new(10_000 + MARGINAL_FEE, 10_000);
        let below_increment = EvictionCost::new(10_000 + MARGINAL_FEE - 1, 10_000);

        assert!(at_increment.exceeds_by_increment(victim));
        assert!(!below_increment.exceeds_by_increment(victim));
        assert!(!victim.exceeds_by_increment(victim));

        // The increment scales with the newcomer's cost.
        let large = EvictionCost::new(2 * (10_000 + MARGINAL_FEE), 20_000);
        assert!(large.exceeds_by_increment(victim));
        let large_short = EvictionCost::new(2 * (10_000 + MARGINAL_FEE) - 1, 20_000);
        assert!(!large_short.exceeds_by_increment(victim));
    }

    #[test]
    fn without_reverses_combine() {
        let parent = EvictionCost::new(10_000, 10_000);
        let child = EvictionCost::new(50_000, 20_000);
        let package = parent.combine(child);

        assert_eq!(package.cost(), 30_000);
        assert_eq!(package.without(child).cost(), 10_000);
        assert_eq!(package.without(child), parent);
    }

    #[test]
    fn extreme_values_do_not_overflow() {
        let max_fee = u64::try_from(MAX_MONEY).expect("MAX_MONEY is positive");
        let max_cost = 80_000_000;

        let rich = EvictionCost::new(max_fee, MEMPOOL_TRANSACTION_COST_THRESHOLD);
        let poor = EvictionCost::new(0, max_cost);

        assert!(poor < rich);
        assert!(rich.exceeds_by_increment(poor));
        assert!(!poor.exceeds_by_increment(rich));

        let saturated = EvictionCost::new(u64::MAX, 1).combine(EvictionCost::new(u64::MAX, 1));
        assert!(saturated > rich);
        assert!(!EvictionCost::new(u64::MAX, u64::MAX).exceeds_by_increment(saturated));
    }
}
