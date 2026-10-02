//! The set of verified transactions in the mempool.

use std::{
    borrow::Cow,
    cmp::Reverse,
    collections::{BTreeSet, HashMap, HashSet},
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

/// The most transactions in a connected group of unconfirmed dependencies.
///
/// Counting parents, children, and siblings bounds both package width and depth.
/// Three transactions have at most three dependency edges, so package scores can
/// be recomputed directly without cached ancestor totals.
pub(super) const MAX_MEMPOOL_PACKAGE_TRANSACTIONS: usize = 3;

/// The most ancestors of a transaction in a bounded dependency group.
pub(super) const MAX_MEMPOOL_ANCESTORS: usize = MAX_MEMPOOL_PACKAGE_TRANSACTIONS - 1;

/// An eviction score, followed by the newest-first insertion time and hash.
type EvictionCandidate = (
    EvictionCost,
    Reverse<Option<chrono::DateTime<chrono::Utc>>>,
    transaction::Hash,
);

#[cfg(feature = "mempool-bench")]
pub(super) mod benchmarks;

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

    /// One current eviction candidate per transaction, ordered cheapest first.
    eviction_order: BTreeSet<EvictionCandidate>,

    #[cfg(test)]
    eviction_score_evaluations: std::sync::atomic::AtomicUsize,

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
        self.eviction_order.clear();
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

    /// Returns the mempool ancestors of `transaction`, or an error if
    /// [`VerifiedSet::insert`] would reject it.
    ///
    /// Two transactions have a spend conflict if they spend the same UTXO or if they reveal the
    /// same nullifier.
    pub fn check_insert(
        &self,
        transaction: &UnminedTx,
        spent_mempool_outpoints: &[transparent::OutPoint],
    ) -> Result<HashSet<transaction::Hash>, SameEffectsTipRejectionError> {
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

        let parents: HashSet<_> = spent_mempool_outpoints
            .iter()
            .map(|outpoint| outpoint.hash)
            .collect();
        let ancestors = self.ancestors(parents);
        if ancestors.len() > MAX_MEMPOOL_ANCESTORS {
            return Err(SameEffectsTipRejectionError::TooManyAncestors);
        }

        if self.dependency_group(ancestors.iter().copied()).len()
            >= MAX_MEMPOOL_PACKAGE_TRANSACTIONS
        {
            return Err(SameEffectsTipRejectionError::TooManyPackageTransactions);
        }

        Ok(ancestors)
    }

    /// Insert a `transaction` into the set.
    ///
    /// Returns an error if `transaction` conflicts with a transaction already in
    /// the set, has a missing mempool output, or would exceed the ancestor or
    /// connected dependency group limit.
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
        let ancestors = self.check_insert(&transaction.transaction, &spent_mempool_outpoints)?;
        let affected = self.dependency_group(ancestors);
        self.remove_eviction_candidates(&affected);

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
        self.insert_eviction_candidates(&self.dependency_group([tx_id]));

        self.report_metrics();

        Ok(())
    }

    /// Selects the packages to evict so that `incoming` fits under `tx_cost_limit`.
    ///
    /// A package is a transaction plus its descendants: the mempool transactions
    /// that directly or indirectly spend its outputs. Removing a transaction
    /// removes its descendants, so the mempool evicts whole packages. It evicts
    /// the package with the lowest [`VerifiedSet::package_score`] first, and
    /// breaks ties by evicting the newest transaction first, then by hash.
    ///
    /// The mempool never evicts `incoming_ancestors`, because evicting them would
    /// invalidate `incoming`.
    ///
    /// Selection stops at the first package that `incoming` does not outbid by
    /// the increment. See [`EvictionCost::exceeds_by_increment`].
    ///
    /// # Performance
    ///
    /// Selection reads the ordered index without scanning the whole mempool.
    /// Each selected victim updates at most three local candidates. For `k`
    /// victims and `n` stored transactions, selection is O(log n + k log k), and
    /// committing the evictions is O(k log n). The index has one entry per
    /// transaction.
    pub(super) fn select_eviction_victims(
        &self,
        incoming: &VerifiedUnminedTx,
        incoming_ancestors: &HashSet<transaction::Hash>,
        tx_cost_limit: u64,
    ) -> EvictionVictims {
        let needed = self
            .total_cost
            .saturating_add(incoming.cost())
            .saturating_sub(tx_cost_limit);
        let incoming = Self::eviction_cost(incoming);

        // Victim planning leaves the persistent index and transactions unchanged.
        let mut unavailable = incoming_ancestors.clone();
        let mut changed: HashSet<transaction::Hash> = HashSet::new();
        let mut updated = BTreeSet::new();
        let mut candidates = self.eviction_order.iter().peekable();
        let mut victims = EvictionVictims::default();
        let mut roots = Vec::new();
        let mut freed = 0;

        while freed < needed {
            // Changed groups use the local index. At most three original entries
            // are skipped per victim, plus the incoming transaction's ancestors.
            while candidates
                .peek()
                .is_some_and(|(_, _, id)| unavailable.contains(id) || changed.contains(id))
            {
                candidates.next();
            }

            let candidate = match (candidates.peek(), updated.first()) {
                (Some(original), Some(local)) if *original <= local => candidates.next().copied(),
                (Some(_), Some(_)) | (None, Some(_)) => updated.pop_first(),
                (Some(_), None) => candidates.next().copied(),
                (None, None) => None,
            };
            let Some((score, _, root)) = candidate else {
                return victims;
            };

            victims.cheapest.get_or_insert(score);
            if !incoming.exceeds_by_increment(score) {
                return victims;
            }

            roots.push(root);
            freed += self.package(&root, &unavailable).cost();
            if freed >= needed {
                break;
            }

            let group = self.dependency_group([root]);
            // Remove earlier local scores before changing the simulated graph.
            for &id in &group {
                if changed.contains(&id) && !unavailable.contains(&id) {
                    updated.remove(&self.eviction_candidate(id, &unavailable));
                }
            }
            let mut removed = self.descendants(root, &unavailable);
            removed.insert(root);
            unavailable.extend(removed);
            changed.extend(&group);
            updated.extend(
                group
                    .into_iter()
                    .filter(|id| !unavailable.contains(id))
                    .map(|id| self.eviction_candidate(id, &unavailable)),
            );
        }

        victims.roots = Some(roots);
        victims
    }

    /// Returns the candidate for a transaction in the simulated remaining pool.
    fn eviction_candidate(
        &self,
        id: transaction::Hash,
        removed: &HashSet<transaction::Hash>,
    ) -> EvictionCandidate {
        (
            self.package_score(&id, removed),
            Reverse(self.transactions[&id].time),
            id,
        )
    }

    /// Removes a group's candidates before mutating transactions or dependencies.
    fn remove_eviction_candidates(&mut self, group: &HashSet<transaction::Hash>) {
        let removed = HashSet::new();
        for &id in group {
            let candidate = self.eviction_candidate(id, &removed);
            assert!(
                self.eviction_order.remove(&candidate),
                "every stored transaction has its current eviction candidate"
            );
        }
    }

    /// Restores candidates for the group's survivors after a mutation.
    fn insert_eviction_candidates(&mut self, group: &HashSet<transaction::Hash>) {
        let removed = HashSet::new();
        for &id in group {
            if self.transactions.contains_key(&id) {
                let candidate = self.eviction_candidate(id, &removed);
                assert!(
                    self.eviction_order.insert(candidate),
                    "a group's old candidates were removed before its mutation"
                );
            }
        }
    }

    /// Returns the eviction score of `tx_id`, excluding removed descendants.
    ///
    /// The score is the higher of the transaction's own eviction cost and the
    /// combined eviction cost of its package:
    ///
    /// - a high-fee descendant raises the eviction cost of its low-fee ancestor, and
    /// - a low-fee descendant does not lower the eviction cost of its high-fee ancestor.
    ///   The descendant is cheaper to evict on its own.
    fn package_score(
        &self,
        tx_id: &transaction::Hash,
        removed: &HashSet<transaction::Hash>,
    ) -> EvictionCost {
        #[cfg(test)]
        self.eviction_score_evaluations
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        Self::eviction_cost(&self.transactions[tx_id]).max(self.package(tx_id, removed))
    }

    /// Returns the combined eviction cost of `tx_id` and its distinct remaining
    /// descendants.
    fn package(
        &self,
        tx_id: &transaction::Hash,
        removed: &HashSet<transaction::Hash>,
    ) -> EvictionCost {
        self.descendants(*tx_id, removed)
            .iter()
            .map(|descendant| Self::eviction_cost(&self.transactions[descendant]))
            .fold(
                Self::eviction_cost(&self.transactions[tx_id]),
                EvictionCost::combine,
            )
    }

    /// Returns the eviction cost of `transaction` on its own.
    pub(super) fn eviction_cost(transaction: &VerifiedUnminedTx) -> EvictionCost {
        EvictionCost::new(transaction.miner_fee.into(), transaction.cost())
    }

    /// Returns the transactions connected to `starts` through parents or children.
    /// Stops at the group limit, which suffices to reject another member.
    fn dependency_group(
        &self,
        starts: impl IntoIterator<Item = transaction::Hash>,
    ) -> HashSet<transaction::Hash> {
        let dependencies = self.transaction_dependencies.dependencies();
        let dependents = self.transaction_dependencies.dependents();
        let mut group = HashSet::new();
        let mut pending: Vec<_> = starts.into_iter().collect();

        while let Some(tx_id) = pending.pop() {
            if !group.insert(tx_id) {
                continue;
            }
            if group.len() >= MAX_MEMPOOL_PACKAGE_TRANSACTIONS {
                break;
            }
            pending.extend(dependencies.get(&tx_id).into_iter().flatten());
            pending.extend(dependents.get(&tx_id).into_iter().flatten());
        }

        group
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

    /// Returns `parents` and the transactions in the set that they directly or indirectly
    /// spend outputs of.
    ///
    /// The walk stops after it finds more than [`MAX_MEMPOOL_ANCESTORS`] transactions, so its
    /// cost stays bounded. Transactions already in the set have at most that many ancestors.
    fn ancestors(
        &self,
        parents: impl IntoIterator<Item = transaction::Hash>,
    ) -> HashSet<transaction::Hash> {
        let dependencies = self.transaction_dependencies.dependencies();
        let mut ancestors = HashSet::new();
        let mut pending: Vec<_> = parents.into_iter().collect();

        while let Some(tx_id) = pending.pop() {
            if ancestors.len() > MAX_MEMPOOL_ANCESTORS {
                break;
            }

            if ancestors.insert(tx_id) {
                pending.extend(dependencies.get(&tx_id).into_iter().flatten());
            }
        }

        ancestors
    }

    /// Clears a list of mined transaction ids from the lists of dependencies for
    /// any other transactions in the mempool and removes their dependents.
    pub fn clear_mined_dependencies(&mut self, mined_ids: &HashSet<transaction::Hash>) {
        let affected: HashSet<_> = mined_ids
            .iter()
            .filter(|id| self.transactions.contains_key(id))
            .flat_map(|&id| self.dependency_group([id]))
            .collect();
        self.remove_eviction_candidates(&affected);
        self.transaction_dependencies
            .clear_mined_dependencies(mined_ids);
        self.insert_eviction_candidates(&affected);
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
        let affected = self.dependency_group([*key_to_remove]);
        self.remove_eviction_candidates(&affected);
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

        self.insert_eviction_candidates(&affected);
        self.report_metrics();
        removed_transactions
    }

    /// Panics if any dependency group exceeds the admission limit.
    #[cfg(test)]
    pub(super) fn assert_dependency_groups_are_bounded(&self) {
        let removed = HashSet::new();
        let expected: BTreeSet<_> = self
            .transactions
            .keys()
            .map(|&id| self.eviction_candidate(id, &removed))
            .collect();
        assert_eq!(self.eviction_order, expected);
        for &start in self.transactions.keys() {
            let mut group = HashSet::new();
            let mut pending = vec![start];
            while let Some(tx_id) = pending.pop() {
                if group.insert(tx_id) {
                    pending.extend(self.transaction_dependencies.direct_dependencies(&tx_id));
                    pending.extend(self.transaction_dependencies.direct_dependents(&tx_id));
                }
            }
            assert!(group.len() <= MAX_MEMPOOL_PACKAGE_TRANSACTIONS);
        }
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
    ///
    /// `None` if the incoming transaction does not outbid them, or does not fit even after
    /// evicting every other package.
    pub roots: Option<Vec<transaction::Hash>>,

    /// The score of the cheapest package that the incoming transaction could evict, or `None`
    /// if there is no such package.
    pub cheapest: Option<EvictionCost>,
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
    fn selection_rescores_only_changed_groups() {
        use std::sync::atomic::Ordering;

        use super::super::fixtures::TxFactory;

        let mut factory = TxFactory::new();
        let mut verified = VerifiedSet::default();
        let mut pending = PendingOutputs::default();
        for _ in 0..1_000 {
            let parent = factory.tx_with(10_000, 1, 0);
            let child = factory.tx_with(20_000, 1, 0);
            let grandchild = factory.tx(30_000);
            let output = |tx: &VerifiedUnminedTx| {
                transparent::OutPoint::from_usize(tx.transaction.id().mined_id(), 0)
            };
            verified
                .insert(parent.clone(), vec![], &mut pending, None)
                .unwrap();
            verified
                .insert(child.clone(), vec![output(&parent)], &mut pending, None)
                .unwrap();
            verified
                .insert(grandchild, vec![output(&child)], &mut pending, None)
                .unwrap();
        }
        let limit = verified.total_cost();
        let before = verified.eviction_order.clone();
        verified
            .eviction_score_evaluations
            .store(0, Ordering::Relaxed);
        let rejected =
            verified.select_eviction_victims(&factory.tx(10_000), &HashSet::new(), limit);
        assert!(rejected.roots.is_none());
        assert_eq!(
            verified.eviction_score_evaluations.load(Ordering::Relaxed),
            0
        );

        let incoming = factory.tx_with(1_000_000, 0, 240_000);
        let victims = verified.select_eviction_victims(&incoming, &HashSet::new(), limit);
        let roots = victims.roots.expect("high fee selects enough victims");
        let max_victims = usize::try_from(
            super::super::super::config::DEFAULT_MAX_TRANSACTION_BYTES
                .div_ceil(MEMPOOL_TRANSACTION_COST_THRESHOLD),
        )
        .expect("default transaction limit fits in usize");
        assert!(roots.len() <= max_victims);
        let local_score_budget = 2 * (MAX_MEMPOOL_PACKAGE_TRANSACTIONS - 1) * roots.len();
        assert!(verified.eviction_score_evaluations.load(Ordering::Relaxed) <= local_score_budget);
        assert_eq!(verified.eviction_order, before);
    }

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
