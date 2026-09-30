//! The set of verified transactions in the mempool.

use std::{
    borrow::Cow,
    cmp::Reverse,
    collections::{HashMap, HashSet},
    hash::Hash,
};

use zakura_chain::{
    block::Height,
    ironwood, orchard, sapling, sprout,
    transaction::{self, UnminedTx, UnminedTxId, VerifiedUnminedTx},
    transparent,
};
use zakura_node_services::mempool::TransactionDependencies;

use crate::components::mempool::pending_outputs::PendingOutputs;

use super::{super::SameEffectsTipRejectionError, eviction_cost::EvictionCost};

// Imports for doc links
#[allow(unused_imports)]
use zakura_chain::transaction::MEMPOOL_TRANSACTION_COST_THRESHOLD;

/// The set of verified transactions stored in the mempool.
///
/// This also caches the all the spent outputs from the transactions in the mempool. The spent
/// outputs include:
///
/// - the dependencies of transactions that spent the outputs of other transactions in the mempool
/// - the outputs of transactions in the mempool
/// - the transparent outpoints spent by transactions in the mempool
/// - the Sprout nullifiers revealed by transactions in the mempool
/// - the Sapling nullifiers revealed by transactions in the mempool
/// - the Orchard nullifiers revealed by transactions in the mempool
/// - the Ironwood nullifiers revealed by transactions in the mempool
#[derive(Default)]
pub struct VerifiedSet {
    /// The set of verified transactions in the mempool.
    transactions: HashMap<transaction::Hash, VerifiedUnminedTx>,

    /// A map of dependencies between transactions in the mempool that
    /// spend or create outputs of other transactions in the mempool.
    transaction_dependencies: TransactionDependencies,

    /// The [`transparent::Output`]s created by verified transactions in the mempool.
    ///
    /// These outputs may be spent by other transactions in the mempool.
    created_outputs: HashMap<transparent::OutPoint, transparent::Output>,

    /// The total size of the transactions in the mempool if they were
    /// serialized.
    transactions_serialized_size: usize,

    /// The total cost of the verified transactions in the set.
    total_cost: u64,

    /// The metric totals for verified transactions in the set.
    metrics: MempoolMetrics,

    /// The set of spent out points by the verified transactions.
    spent_outpoints: HashSet<transparent::OutPoint>,

    /// The set of revealed Sprout nullifiers.
    sprout_nullifiers: HashSet<sprout::Nullifier>,

    /// The set of revealed Sapling nullifiers.
    sapling_nullifiers: HashSet<sapling::Nullifier>,

    /// The set of revealed Orchard nullifiers.
    orchard_nullifiers: HashSet<orchard::Nullifier>,

    /// The set of revealed Ironwood nullifiers.
    ironwood_nullifiers: HashSet<ironwood::Nullifier>,
}

impl Drop for VerifiedSet {
    fn drop(&mut self) {
        // zero the metrics on drop
        self.clear()
    }
}

impl VerifiedSet {
    /// Returns a reference to the [`HashMap`] of [`VerifiedUnminedTx`]s in the set.
    pub fn transactions(&self) -> &HashMap<transaction::Hash, VerifiedUnminedTx> {
        &self.transactions
    }

    /// Returns a reference to the [`TransactionDependencies`] in the set.
    pub fn transaction_dependencies(&self) -> &TransactionDependencies {
        &self.transaction_dependencies
    }

    /// Returns a [`transparent::Output`] created by a mempool transaction for the provided
    /// [`transparent::OutPoint`] if one exists, or None otherwise.
    pub fn created_output(&self, outpoint: &transparent::OutPoint) -> Option<transparent::Output> {
        self.created_outputs.get(outpoint).cloned()
    }

    /// Returns true if a tx in the set has spent the output at the provided outpoint.
    pub fn has_spent_outpoint(&self, outpoint: &transparent::OutPoint) -> bool {
        self.spent_outpoints.contains(outpoint)
    }

    /// Returns the number of verified transactions in the set.
    pub fn transaction_count(&self) -> usize {
        self.transactions.len()
    }

    /// Returns the total cost of the verified transactions in the set.
    ///
    /// [ZIP-401]: https://zips.z.cash/zip-0401
    pub fn total_cost(&self) -> u64 {
        self.total_cost
    }

    /// Returns the total serialized size of the verified transactions in the set.
    ///
    /// This can be less than the total cost, because the minimum transaction cost
    /// is based on the [`MEMPOOL_TRANSACTION_COST_THRESHOLD`].
    pub fn total_serialized_size(&self) -> usize {
        self.transactions_serialized_size
    }

    /// Returns `true` if the set of verified transactions contains the transaction with the
    /// specified [`transaction::Hash`].
    pub fn contains(&self, id: &transaction::Hash) -> bool {
        self.transactions.contains_key(id)
    }

    /// Clear the set of verified transactions.
    ///
    /// Also clears all internal caches.
    pub fn clear(&mut self) {
        self.transactions.clear();
        self.transaction_dependencies.clear();
        self.spent_outpoints.clear();
        self.sprout_nullifiers.clear();
        self.sapling_nullifiers.clear();
        self.orchard_nullifiers.clear();
        self.ironwood_nullifiers.clear();
        self.created_outputs.clear();
        self.transactions_serialized_size = 0;
        self.total_cost = 0;
        self.metrics = MempoolMetrics::default();
        self.report_metrics();
    }

    /// Returns an error if [`VerifiedSet::insert`] would reject `transaction`.
    ///
    /// Two transactions have a spend conflict if they spend the same UTXO or if they reveal the
    /// same nullifier.
    pub fn check_insert(
        &self,
        transaction: &UnminedTx,
        spent_mempool_outpoints: &[transparent::OutPoint],
    ) -> Result<(), SameEffectsTipRejectionError> {
        if self.has_spend_conflicts(transaction) {
            return Err(SameEffectsTipRejectionError::SpendConflict);
        }

        // This likely only needs to check that the transaction hash of the outpoint is still in the mempool,
        // but it's likely rare that a transaction spends multiple transparent outputs of
        // a single transaction in practice.
        for outpoint in spent_mempool_outpoints {
            if !self.created_outputs.contains_key(outpoint) {
                return Err(SameEffectsTipRejectionError::MissingOutput);
            }
        }

        Ok(())
    }

    /// Insert a `transaction` into the set.
    ///
    /// Returns an error if the `transaction` has spend conflicts with any other transaction
    /// already in the set.
    ///
    /// Two transactions have a spend conflict if they spend the same UTXO or if they reveal the
    /// same nullifier.
    pub fn insert(
        &mut self,
        mut transaction: VerifiedUnminedTx,
        spent_mempool_outpoints: Vec<transparent::OutPoint>,
        pending_outputs: &mut PendingOutputs,
        height: Option<Height>,
    ) -> Result<(), SameEffectsTipRejectionError> {
        self.check_insert(&transaction.transaction, &spent_mempool_outpoints)?;

        let tx_id = transaction.transaction.id().mined_id();
        self.transaction_dependencies
            .add(tx_id, spent_mempool_outpoints);

        // Inserts the transaction's outputs into the internal caches and responds to pending output requests.
        let tx = &transaction.transaction.transaction();
        for (index, output) in tx.outputs().iter().cloned().enumerate() {
            let outpoint = transparent::OutPoint::from_usize(tx_id, index);
            self.created_outputs.insert(outpoint, output.clone());
            pending_outputs.respond(&outpoint, output)
        }
        self.spent_outpoints.extend(tx.spent_outpoints());
        self.sprout_nullifiers.extend(tx.sprout_nullifiers());
        self.sapling_nullifiers.extend(tx.sapling_nullifiers());
        self.orchard_nullifiers.extend(tx.orchard_nullifiers());
        self.ironwood_nullifiers.extend(tx.ironwood_nullifiers());

        self.transactions_serialized_size += transaction.transaction.size();
        self.total_cost += transaction.cost();
        transaction.time = Some(chrono::Utc::now());
        transaction.height = height;
        self.metrics.add_transaction(&transaction);
        self.transactions.insert(tx_id, transaction);

        self.report_metrics();

        Ok(())
    }

    /// Selects the packages to evict so that a transaction with `incoming_cost` fits under
    /// `tx_cost_limit`.
    ///
    /// A package is a transaction plus its descendants: the mempool transactions that directly
    /// or indirectly spend its outputs. Removing a transaction removes its descendants, so the
    /// mempool evicts whole packages. It evicts the package with the lowest
    /// [`VerifiedSet::package_eviction_costs`] first, and breaks ties by evicting the newest
    /// transaction first, then by transaction hash.
    ///
    /// The mempool ancestors of the incoming transaction, found from
    /// `spent_mempool_outpoints`, are never victims, because evicting them would invalidate
    /// the incoming transaction.
    ///
    /// Returns `None` if the incoming transaction does not fit even after evicting every other
    /// package.
    ///
    /// # Performance
    ///
    /// Each victim costs an O(n + edges) pass over the set, where n is at most
    /// `tx_cost_limit / MEMPOOL_TRANSACTION_COST_THRESHOLD`. The mempool only calls this
    /// method when it is full.
    pub(super) fn select_eviction_victims(
        &self,
        spent_mempool_outpoints: &[transparent::OutPoint],
        incoming_cost: u64,
        tx_cost_limit: u64,
    ) -> Option<EvictionVictims> {
        let needed = self
            .total_cost
            .saturating_add(incoming_cost)
            .saturating_sub(tx_cost_limit);

        // Victims and their descendants, plus the incoming transaction's ancestors.
        let mut unavailable = self.mempool_ancestors(spent_mempool_outpoints);
        let mut victims = EvictionVictims::default();
        let mut freed = 0;

        while freed < needed {
            // Recompute every package after each pick, because evicting a package lowers the
            // eviction cost of any ancestor that shared its descendants.
            let packages = self.package_eviction_costs(&unavailable);
            let (eviction_cost, _, root) = packages
                .into_iter()
                .map(|(tx_id, eviction_cost)| {
                    (
                        eviction_cost,
                        Reverse(self.transactions[&tx_id].time),
                        tx_id,
                    )
                })
                .min()?;

            for tx_id in self.descendants(root, &unavailable) {
                freed += self.transactions[&tx_id].cost();
                unavailable.insert(tx_id);
            }
            freed += self.transactions[&root].cost();
            unavailable.insert(root);

            victims.cheapest.get_or_insert(eviction_cost);
            victims.highest = victims.highest.max(Some(eviction_cost));
            victims.roots.push(root);
        }

        Some(victims)
    }

    /// Returns the package eviction cost of every transaction in the set that is not in
    /// `removed`.
    ///
    /// The package eviction cost of a transaction is the higher of its own eviction cost and
    /// the combined eviction cost of the transaction and its descendants:
    ///
    /// - a high-fee descendant raises the eviction cost of its low-fee ancestor, and
    /// - a low-fee descendant does not lower the eviction cost of its high-fee ancestor.
    ///   The descendant is cheaper to evict on its own.
    ///
    /// One pass computes every package in O(n + edges), even for long chains of mempool
    /// transactions. A descendant that a transaction reaches along several paths counts once
    /// per path. Such diamonds are rare, and the extra count only moves the package toward
    /// that descendant's eviction cost.
    fn package_eviction_costs(
        &self,
        removed: &HashSet<transaction::Hash>,
    ) -> HashMap<transaction::Hash, EvictionCost> {
        let dependents = self.transaction_dependencies.dependents();
        let children = |tx_id: &transaction::Hash| {
            dependents
                .get(tx_id)
                .into_iter()
                .flatten()
                .filter(|dependent| {
                    !removed.contains(*dependent) && self.transactions.contains_key(*dependent)
                })
        };

        // The combined eviction cost of each transaction and its descendants.
        let mut packages: HashMap<transaction::Hash, EvictionCost> =
            HashMap::with_capacity(self.transactions.len());

        for &start in self.transactions.keys() {
            if removed.contains(&start) {
                continue;
            }

            // Visit each transaction after its children, without recursion, because
            // mempool chains can be thousands of transactions long.
            let mut pending = vec![(start, false)];
            while let Some((tx_id, children_done)) = pending.pop() {
                if packages.contains_key(&tx_id) {
                    continue;
                }

                if children_done {
                    let package = children(&tx_id)
                        .filter_map(|child| packages.get(child))
                        .fold(
                            Self::eviction_cost(&self.transactions[&tx_id]),
                            |package, child| package.combine(*child),
                        );
                    packages.insert(tx_id, package);
                } else {
                    pending.push((tx_id, true));
                    pending.extend(
                        children(&tx_id)
                            .filter(|child| !packages.contains_key(*child))
                            .map(|&child| (child, false)),
                    );
                }
            }
        }

        packages
            .into_iter()
            .map(|(tx_id, package)| {
                let own = Self::eviction_cost(&self.transactions[&tx_id]);
                (tx_id, own.max(package))
            })
            .collect()
    }

    /// Returns the eviction cost of `transaction` on its own.
    pub(super) fn eviction_cost(transaction: &VerifiedUnminedTx) -> EvictionCost {
        EvictionCost::new(transaction.miner_fee.into(), transaction.cost())
    }

    /// Returns the transactions in the set that directly or indirectly spend outputs of `tx_id`,
    /// skipping transactions in `removed`.
    ///
    /// Every returned transaction is in the set.
    fn descendants(
        &self,
        tx_id: transaction::Hash,
        removed: &HashSet<transaction::Hash>,
    ) -> HashSet<transaction::Hash> {
        let dependents = self.transaction_dependencies.dependents();
        let mut descendants = HashSet::new();
        let mut pending = vec![tx_id];

        while let Some(tx_id) = pending.pop() {
            for &dependent in dependents.get(&tx_id).into_iter().flatten() {
                if !removed.contains(&dependent)
                    && self.transactions.contains_key(&dependent)
                    && descendants.insert(dependent)
                {
                    pending.push(dependent);
                }
            }
        }

        descendants
    }

    /// Returns the transactions in the set that directly or indirectly create the outputs in
    /// `spent_mempool_outpoints`.
    fn mempool_ancestors(
        &self,
        spent_mempool_outpoints: &[transparent::OutPoint],
    ) -> HashSet<transaction::Hash> {
        let dependencies = self.transaction_dependencies.dependencies();
        let mut ancestors = HashSet::new();
        let mut pending: Vec<_> = spent_mempool_outpoints
            .iter()
            .map(|outpoint| outpoint.hash)
            .collect();

        while let Some(tx_id) = pending.pop() {
            if ancestors.insert(tx_id) {
                pending.extend(dependencies.get(&tx_id).into_iter().flatten());
            }
        }

        ancestors
    }

    /// Clears a list of mined transaction ids from the lists of dependencies for
    /// any other transactions in the mempool and removes their dependents.
    pub fn clear_mined_dependencies(&mut self, mined_ids: &HashSet<transaction::Hash>) {
        self.transaction_dependencies
            .clear_mined_dependencies(mined_ids);
    }

    /// Removes all transactions in the set that match the `predicate`.
    ///
    /// Returns the amount of transactions removed.
    pub fn remove_all_that(
        &mut self,
        predicate: impl Fn(&VerifiedUnminedTx) -> bool,
    ) -> HashSet<UnminedTxId> {
        let keys_to_remove: Vec<_> = self
            .transactions
            .iter()
            .filter_map(|(&tx_id, tx)| predicate(tx).then_some(tx_id))
            .collect();

        let mut removed_transactions = HashSet::new();

        for key_to_remove in keys_to_remove {
            if !self.transactions.contains_key(&key_to_remove) {
                // Skip any keys that may have already been removed as their dependencies were removed.
                continue;
            }

            removed_transactions.extend(
                self.remove(&key_to_remove)
                    .into_iter()
                    .map(|tx| tx.transaction.id()),
            );
        }

        removed_transactions
    }

    /// Accepts a transaction id for a transaction to remove from the verified set.
    ///
    /// Removes the transaction and any transactions that directly or indirectly
    /// depend on it from the set.
    ///
    /// Returns a list of transactions that have been removed with the target transaction
    /// as the last item.
    ///
    /// Also removes the outputs of any removed transactions from the internal caches.
    pub(super) fn remove(&mut self, key_to_remove: &transaction::Hash) -> Vec<VerifiedUnminedTx> {
        let removed_transactions: Vec<_> = self
            .transaction_dependencies
            .remove_all(key_to_remove)
            .iter()
            .chain(std::iter::once(key_to_remove))
            .filter_map(|key_to_remove| {
                let Some(removed_tx) = self.transactions.remove(key_to_remove) else {
                    tracing::warn!(?key_to_remove, "invalid transaction key");
                    return None;
                };

                self.transactions_serialized_size -= removed_tx.transaction.size();
                self.total_cost -= removed_tx.cost();
                self.metrics.remove_transaction(&removed_tx);
                self.remove_outputs(&removed_tx.transaction);

                Some(removed_tx)
            })
            .collect();

        self.report_metrics();
        removed_transactions
    }

    /// Returns `true` if the given `transaction` has any spend conflicts with transactions in the
    /// mempool.
    ///
    /// Two transactions have a spend conflict if they spend the same UTXO or if they reveal the
    /// same nullifier.
    fn has_spend_conflicts(&self, unmined_tx: &UnminedTx) -> bool {
        let tx = unmined_tx.transaction();

        Self::has_conflicts(&self.spent_outpoints, tx.spent_outpoints())
            || Self::has_conflicts(&self.sprout_nullifiers, tx.sprout_nullifiers().copied())
            || Self::has_conflicts(&self.sapling_nullifiers, tx.sapling_nullifiers().copied())
            || Self::has_conflicts(&self.orchard_nullifiers, tx.orchard_nullifiers().copied())
            || Self::has_conflicts(&self.ironwood_nullifiers, tx.ironwood_nullifiers().copied())
    }

    /// Removes the tracked transaction outputs from the mempool.
    fn remove_outputs(&mut self, unmined_tx: &UnminedTx) {
        let tx = unmined_tx.transaction();

        for index in 0..tx.outputs().len() {
            self.created_outputs
                .remove(&transparent::OutPoint::from_usize(
                    unmined_tx.id().mined_id(),
                    index,
                ));
        }

        let spent_outpoints = tx.spent_outpoints().map(Cow::Owned);
        let sprout_nullifiers = tx.sprout_nullifiers().map(Cow::Borrowed);
        let sapling_nullifiers = tx.sapling_nullifiers().map(Cow::Borrowed);
        let orchard_nullifiers = tx.orchard_nullifiers().map(Cow::Borrowed);
        let ironwood_nullifiers = tx.ironwood_nullifiers().map(Cow::Borrowed);

        Self::remove_from_set(&mut self.spent_outpoints, spent_outpoints);
        Self::remove_from_set(&mut self.sprout_nullifiers, sprout_nullifiers);
        Self::remove_from_set(&mut self.sapling_nullifiers, sapling_nullifiers);
        Self::remove_from_set(&mut self.orchard_nullifiers, orchard_nullifiers);
        Self::remove_from_set(&mut self.ironwood_nullifiers, ironwood_nullifiers);
    }

    /// Returns `true` if the two sets have common items.
    fn has_conflicts<T>(set: &HashSet<T>, mut list: impl Iterator<Item = T>) -> bool
    where
        T: Eq + Hash,
    {
        list.any(|item| set.contains(&item))
    }

    /// Removes some items from a [`HashSet`].
    ///
    /// Each item in the list of `items` should be wrapped in a [`Cow`]. This allows this generic
    /// method to support both borrowed and owned items.
    fn remove_from_set<'t, T>(set: &mut HashSet<T>, items: impl IntoIterator<Item = Cow<'t, T>>)
    where
        T: Clone + Eq + Hash + 't,
    {
        for item in items {
            set.remove(&item);
        }
    }

    /// Report the current mempool metrics.
    fn report_metrics(&self) {
        metrics::gauge!(
            "zcash.mempool.actions.unpaid",
            "bk" => "< 0.2",
        )
        .set(self.metrics.unpaid_actions_with_weight_lt20pct as f64);
        metrics::gauge!(
            "zcash.mempool.actions.unpaid",
            "bk" => "< 0.4",
        )
        .set(self.metrics.unpaid_actions_with_weight_lt40pct as f64);
        metrics::gauge!(
            "zcash.mempool.actions.unpaid",
            "bk" => "< 0.6",
        )
        .set(self.metrics.unpaid_actions_with_weight_lt60pct as f64);
        metrics::gauge!(
            "zcash.mempool.actions.unpaid",
            "bk" => "< 0.8",
        )
        .set(self.metrics.unpaid_actions_with_weight_lt80pct as f64);
        metrics::gauge!(
            "zcash.mempool.actions.unpaid",
            "bk" => "< 1",
        )
        .set(self.metrics.unpaid_actions_with_weight_lt1 as f64);
        metrics::gauge!("zcash.mempool.actions.paid").set(self.metrics.paid_actions as f64);
        metrics::gauge!("zcash.mempool.size.transactions",).set(self.transaction_count() as f64);
        metrics::gauge!(
            "zcash.mempool.size.weighted",
            "bk" => "< 1",
        )
        .set(self.metrics.size_with_weight_lt1 as f64);
        metrics::gauge!(
            "zcash.mempool.size.weighted",
            "bk" => "1",
        )
        .set(self.metrics.size_with_weight_eq1 as f64);
        metrics::gauge!(
            "zcash.mempool.size.weighted",
            "bk" => "> 1",
        )
        .set(self.metrics.size_with_weight_gt1 as f64);
        metrics::gauge!(
            "zcash.mempool.size.weighted",
            "bk" => "> 2",
        )
        .set(self.metrics.size_with_weight_gt2 as f64);
        metrics::gauge!(
            "zcash.mempool.size.weighted",
            "bk" => "> 3",
        )
        .set(self.metrics.size_with_weight_gt3 as f64);
        metrics::gauge!("zcash.mempool.size.bytes",).set(self.transactions_serialized_size as f64);
        metrics::gauge!("zcash.mempool.cost.bytes").set(self.total_cost as f64);
    }
}

/// The packages that the mempool evicts to make room for an incoming transaction.
///
/// See [`VerifiedSet::select_eviction_victims`].
#[derive(Debug, Default)]
pub(super) struct EvictionVictims {
    /// The transactions to evict with their descendants, cheapest first.
    pub roots: Vec<transaction::Hash>,

    /// The eviction cost of the cheapest package in the mempool, or `None` if no eviction is
    /// needed.
    pub cheapest: Option<EvictionCost>,

    /// The highest eviction cost among the evicted packages, or `None` if no eviction is
    /// needed.
    pub highest: Option<EvictionCost>,
}

/// The aggregate values for mempool metrics.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct MempoolMetrics {
    unpaid_actions_with_weight_lt20pct: u32,
    unpaid_actions_with_weight_lt40pct: u32,
    unpaid_actions_with_weight_lt60pct: u32,
    unpaid_actions_with_weight_lt80pct: u32,
    unpaid_actions_with_weight_lt1: u32,
    paid_actions: u32,
    size_with_weight_lt1: usize,
    size_with_weight_eq1: usize,
    size_with_weight_gt1: usize,
    size_with_weight_gt2: usize,
    size_with_weight_gt3: usize,
}

impl MempoolMetrics {
    /// Add a verified transaction's contribution to the metric totals.
    fn add_transaction(&mut self, transaction: &VerifiedUnminedTx) {
        self.add(Self::for_transaction(transaction));
    }

    /// Remove a verified transaction's contribution from the metric totals.
    fn remove_transaction(&mut self, transaction: &VerifiedUnminedTx) {
        self.remove(Self::for_transaction(transaction));
    }

    /// Return the metric contribution for a verified transaction.
    fn for_transaction(transaction: &VerifiedUnminedTx) -> Self {
        Self::for_values(
            transaction.fee_weight_ratio,
            transaction.conventional_actions,
            transaction.unpaid_actions,
            transaction.transaction.size(),
        )
    }

    /// Return the metric contribution for transaction metric values.
    fn for_values(
        fee_weight_ratio: f32,
        conventional_actions: u32,
        unpaid_actions: u32,
        size: usize,
    ) -> Self {
        let mut metrics = Self {
            paid_actions: conventional_actions - unpaid_actions,
            ..Default::default()
        };

        if fee_weight_ratio > 3.0 {
            metrics.size_with_weight_gt3 = size;
        } else if fee_weight_ratio > 2.0 {
            metrics.size_with_weight_gt2 = size;
        } else if fee_weight_ratio > 1.0 {
            metrics.size_with_weight_gt1 = size;
        } else if fee_weight_ratio == 1.0 {
            metrics.size_with_weight_eq1 = size;
        } else {
            metrics.size_with_weight_lt1 = size;
            if fee_weight_ratio < 0.2 {
                metrics.unpaid_actions_with_weight_lt20pct = unpaid_actions;
            } else if fee_weight_ratio < 0.4 {
                metrics.unpaid_actions_with_weight_lt40pct = unpaid_actions;
            } else if fee_weight_ratio < 0.6 {
                metrics.unpaid_actions_with_weight_lt60pct = unpaid_actions;
            } else if fee_weight_ratio < 0.8 {
                metrics.unpaid_actions_with_weight_lt80pct = unpaid_actions;
            } else {
                metrics.unpaid_actions_with_weight_lt1 = unpaid_actions;
            }
        }

        metrics
    }

    /// Add another set of metric totals to this set.
    fn add(&mut self, other: Self) {
        self.unpaid_actions_with_weight_lt20pct += other.unpaid_actions_with_weight_lt20pct;
        self.unpaid_actions_with_weight_lt40pct += other.unpaid_actions_with_weight_lt40pct;
        self.unpaid_actions_with_weight_lt60pct += other.unpaid_actions_with_weight_lt60pct;
        self.unpaid_actions_with_weight_lt80pct += other.unpaid_actions_with_weight_lt80pct;
        self.unpaid_actions_with_weight_lt1 += other.unpaid_actions_with_weight_lt1;
        self.paid_actions += other.paid_actions;
        self.size_with_weight_lt1 += other.size_with_weight_lt1;
        self.size_with_weight_eq1 += other.size_with_weight_eq1;
        self.size_with_weight_gt1 += other.size_with_weight_gt1;
        self.size_with_weight_gt2 += other.size_with_weight_gt2;
        self.size_with_weight_gt3 += other.size_with_weight_gt3;
    }

    /// Remove another set of metric totals from this set.
    fn remove(&mut self, other: Self) {
        self.unpaid_actions_with_weight_lt20pct -= other.unpaid_actions_with_weight_lt20pct;
        self.unpaid_actions_with_weight_lt40pct -= other.unpaid_actions_with_weight_lt40pct;
        self.unpaid_actions_with_weight_lt60pct -= other.unpaid_actions_with_weight_lt60pct;
        self.unpaid_actions_with_weight_lt80pct -= other.unpaid_actions_with_weight_lt80pct;
        self.unpaid_actions_with_weight_lt1 -= other.unpaid_actions_with_weight_lt1;
        self.paid_actions -= other.paid_actions;
        self.size_with_weight_lt1 -= other.size_with_weight_lt1;
        self.size_with_weight_eq1 -= other.size_with_weight_eq1;
        self.size_with_weight_gt1 -= other.size_with_weight_gt1;
        self.size_with_weight_gt2 -= other.size_with_weight_gt2;
        self.size_with_weight_gt3 -= other.size_with_weight_gt3;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clear_removes_ironwood_nullifiers() {
        let mut verified = VerifiedSet::default();

        verified.ironwood_nullifiers.insert(
            ironwood::Nullifier::try_from([0; 32]).expect("zero is a valid Pallas base field"),
        );

        verified.clear();

        assert!(verified.ironwood_nullifiers.is_empty());
    }

    #[test]
    fn metrics_are_updated_incrementally() {
        let transaction_metrics = [
            (0.1, 5, 4, 10),
            (0.2, 6, 3, 20),
            (0.4, 7, 2, 30),
            (0.6, 8, 1, 40),
            (0.8, 9, 0, 50),
            (1.0, 10, 0, 60),
            (1.5, 11, 0, 70),
            (2.5, 12, 0, 80),
            (3.5, 13, 0, 90),
        ];
        let mut metrics = MempoolMetrics::default();

        for &(fee_weight_ratio, conventional_actions, unpaid_actions, size) in &transaction_metrics
        {
            metrics.add(MempoolMetrics::for_values(
                fee_weight_ratio,
                conventional_actions,
                unpaid_actions,
                size,
            ));
        }

        assert_eq!(
            metrics,
            MempoolMetrics {
                unpaid_actions_with_weight_lt20pct: 4,
                unpaid_actions_with_weight_lt40pct: 3,
                unpaid_actions_with_weight_lt60pct: 2,
                unpaid_actions_with_weight_lt80pct: 1,
                unpaid_actions_with_weight_lt1: 0,
                paid_actions: 71,
                size_with_weight_lt1: 150,
                size_with_weight_eq1: 60,
                size_with_weight_gt1: 70,
                size_with_weight_gt2: 80,
                size_with_weight_gt3: 90,
            }
        );

        for &(fee_weight_ratio, conventional_actions, unpaid_actions, size) in
            transaction_metrics.iter().rev()
        {
            metrics.remove(MempoolMetrics::for_values(
                fee_weight_ratio,
                conventional_actions,
                unpaid_actions,
                size,
            ));
        }

        assert_eq!(metrics, MempoolMetrics::default());
    }
}
