//! The set of verified transactions in the mempool.

use std::{borrow::Cow, cmp::Reverse, collections::BTreeSet, hash::Hash};

use chrono::{DateTime, Utc};
use zakura_rt::collections::{HashMap, HashSet};

use zakura_chain::{
    block::Height,
    ironwood, orchard, sapling,
    transaction::{self, Transaction, UnminedTx, UnminedTxId, VerifiedUnminedTx},
    transparent,
};
use zakura_node_services::mempool::{port::RemovalReason, TransactionDependencies};

use super::{eviction_cost::EvictionCost, SameEffectsTipRejectionError};

/// The most transactions in a connected group of unconfirmed dependencies.
///
/// Counting parents, children, and siblings bounds both package width and depth.
/// Three transactions have at most three dependency edges, so package scores can
/// be recomputed directly without cached ancestor totals.
pub const MAX_MEMPOOL_PACKAGE_TRANSACTIONS: usize = 3;

/// The most ancestors of a transaction in a bounded dependency group.
pub const MAX_MEMPOOL_ANCESTORS: usize = MAX_MEMPOOL_PACKAGE_TRANSACTIONS - 1;

/// An eviction score, followed by the newest-first insertion time and hash.
type EvictionCandidate = (
    EvictionCost,
    Reverse<Option<DateTime<Utc>>>,
    transaction::Hash,
);

/// An ancestor-package fee per cost, highest first, followed by the oldest
/// insertion time and transaction hash.
type MiningCandidate = (
    Reverse<EvictionCost>,
    Option<DateTime<Utc>>,
    transaction::Hash,
);

/// What the set keeps about each transaction beside its body: the scores
/// stored in the two ordering indexes, so removal does not reconstruct keys
/// from the graph, and its insertion stamp.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Entry {
    eviction: EvictionCost,
    mining: EvictionCost,
    /// See [`VerifiedSet::added_at`].
    added_at: u64,
}

#[cfg(test)]
mod mining_tests;

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
/// - the Sapling nullifiers revealed by transactions in the mempool
/// - the Orchard nullifiers revealed by transactions in the mempool
/// - the Ironwood nullifiers revealed by transactions in the mempool
#[derive(Default)]
pub struct VerifiedSet {
    /// The set of verified transactions in the mempool.
    transactions: HashMap<transaction::Hash, VerifiedUnminedTx>,

    /// One current eviction candidate per transaction, ordered cheapest first.
    eviction_order: BTreeSet<EvictionCandidate>,

    /// One candidate per transaction, scored with its unconfirmed ancestors.
    /// Higher fee per cost comes first, with older insertion times breaking ties.
    mining_order: BTreeSet<MiningCandidate>,

    /// Each transaction's [`Entry`].
    entries: HashMap<transaction::Hash, Entry>,

    /// Expiring transactions ordered by height, excluding no-expiry values.
    expiry_order: BTreeSet<(Height, transaction::Hash)>,

    /// Insertions since the set was made. It never goes back, a clear
    /// included, so a later insertion always has a larger stamp.
    insertions: u64,

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

    /// The fee-weight totals for verified transactions in the set.
    weights: WeightTotals,

    /// Owners of transparent outpoints spent by verified transactions.
    spent_outpoints: HashMap<transparent::OutPoint, transaction::Hash>,

    /// Owners of revealed Sapling nullifiers.
    sapling_nullifiers: HashMap<sapling::Nullifier, transaction::Hash>,

    /// Owners of revealed Orchard nullifiers.
    orchard_nullifiers: HashMap<orchard::Nullifier, transaction::Hash>,

    /// Owners of revealed Ironwood nullifiers.
    ironwood_nullifiers: HashMap<ironwood::Nullifier, transaction::Hash>,
}

impl VerifiedSet {
    /// Returns a reference to the [`HashMap`] of [`VerifiedUnminedTx`]s in the set.
    pub fn transactions(&self) -> &HashMap<transaction::Hash, VerifiedUnminedTx> {
        &self.transactions
    }

    /// Returns transactions by descending ancestor-package fee per cost, then
    /// ascending insertion time and TXID.
    ///
    /// Each package includes the transaction and all its distinct unconfirmed
    /// ancestors. These are baseline scores before a template selects parents;
    /// the ordering does not itself put parents before children.
    pub(super) fn mining_transactions(&self) -> impl Iterator<Item = &VerifiedUnminedTx> {
        self.mining_order
            .iter()
            .map(|(_, _, id)| &self.transactions[id])
    }

    /// The mined ids of [`Self::mining_transactions`], in the same order.
    pub(super) fn mining_order(&self) -> impl Iterator<Item = transaction::Hash> + '_ {
        self.mining_order.iter().map(|(_, _, id)| *id)
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

    /// The sizes and unpaid actions of the transactions in the set, by fee
    /// weight.
    pub fn weights(&self) -> &WeightTotals {
        &self.weights
    }

    /// Returns `true` if the set of verified transactions contains the transaction with the
    /// specified [`transaction::Hash`].
    pub fn contains(&self, id: &transaction::Hash) -> bool {
        self.transactions.contains_key(id)
    }

    /// The stamp the set gave the stored transaction `id` when it was
    /// inserted: the number of insertions so far, that one included. A
    /// transaction inserted after [`Self::insertions`] read `n` has a stamp
    /// above `n`.
    pub fn added_at(&self, id: &transaction::Hash) -> Option<u64> {
        self.entries.get(id).map(|entry| entry.added_at)
    }

    /// Insertions since the set was made. See [`Self::added_at`].
    pub fn insertions(&self) -> u64 {
        self.insertions
    }

    /// Clear the set of verified transactions.
    ///
    /// Also clears all internal caches. The insertion count stays.
    pub fn clear(&mut self) {
        self.transactions.clear();
        self.eviction_order.clear();
        self.mining_order.clear();
        self.entries.clear();
        self.expiry_order.clear();
        self.transaction_dependencies.clear();
        self.spent_outpoints.clear();
        self.sapling_nullifiers.clear();
        self.orchard_nullifiers.clear();
        self.ironwood_nullifiers.clear();
        self.created_outputs.clear();
        self.transactions_serialized_size = 0;
        self.total_cost = 0;
        self.weights = WeightTotals::default();
    }

    /// Removes and returns every stored transaction, oldest first. See
    /// [`Self::clear`].
    pub(super) fn take_all(&mut self) -> Vec<VerifiedUnminedTx> {
        let order = self.oldest_first(self.transactions.keys().copied());
        let mut transactions = std::mem::take(&mut self.transactions);
        self.clear();
        order
            .iter()
            .map(|id| {
                transactions
                    .remove(id)
                    .expect("the order names stored transactions")
            })
            .collect()
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
    ///
    /// `time` is stamped on the transaction as its insertion time, and the
    /// set stamps it with its insertion count. See [`Self::added_at`].
    pub fn insert(
        &mut self,
        mut transaction: VerifiedUnminedTx,
        spent_mempool_outpoints: Vec<transparent::OutPoint>,
        height: Option<Height>,
        time: DateTime<Utc>,
    ) -> Result<(), SameEffectsTipRejectionError> {
        let ancestors = self.check_insert(&transaction.transaction, &spent_mempool_outpoints)?;
        let tx_id = transaction.transaction.id().mined_id();
        self.transaction_dependencies
            .add(tx_id, spent_mempool_outpoints);

        // Inserts the transaction's outputs into the internal caches.
        let tx = &transaction.transaction.transaction();
        if let Some(expiry_height) = tx.expiry_height() {
            self.expiry_order.insert((expiry_height, tx_id));
        }
        for (index, output) in tx.outputs().iter().cloned().enumerate() {
            let outpoint = transparent::OutPoint::from_usize(tx_id, index);
            self.created_outputs.insert(outpoint, output);
        }
        self.spent_outpoints
            .extend(tx.spent_outpoints().map(|outpoint| (outpoint, tx_id)));
        self.sapling_nullifiers
            .extend(tx.sapling_nullifiers().map(|&nullifier| (nullifier, tx_id)));
        self.orchard_nullifiers
            .extend(tx.orchard_nullifiers().map(|&nullifier| (nullifier, tx_id)));
        self.ironwood_nullifiers.extend(
            tx.ironwood_nullifiers()
                .map(|&nullifier| (nullifier, tx_id)),
        );

        self.transactions_serialized_size += transaction.transaction.size();
        self.total_cost += transaction.cost();
        transaction.time = Some(time);
        transaction.height = height;
        self.weights.add_transaction(&transaction);
        self.transactions.insert(tx_id, transaction);
        self.insertions = self.insertions.saturating_add(1);
        self.insert_ordering_candidate(tx_id, self.insertions);
        self.refresh_eviction_candidates(&ancestors);

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
    /// Both `incoming` on its own and its combined rate with every unconfirmed
    /// ancestor must outbid each victim by the increment. This prevents protected
    /// ancestors from diluting the rate of the package retained after admission.
    /// Selection stops at the first victim that fails either comparison.
    /// See [`EvictionCost::exceeds_by_increment`].
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
        // check_insert has bounded and deduplicated these protected ancestors.
        let incoming_package = incoming_ancestors
            .iter()
            .map(|id| Self::eviction_cost(&self.transactions[id]))
            .fold(incoming, EvictionCost::combine);
        let incoming = incoming.min(incoming_package);

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

    /// Returns the mining candidate using all distinct unconfirmed ancestors.
    fn mining_candidate(&self, id: transaction::Hash) -> MiningCandidate {
        let score = self
            .transaction_ancestors(id)
            .iter()
            .map(|ancestor| Self::eviction_cost(&self.transactions[ancestor]))
            .fold(
                Self::eviction_cost(&self.transactions[&id]),
                EvictionCost::combine,
            );
        (Reverse(score), self.transactions[&id].time, id)
    }

    /// Removes a transaction's cached ordering entries and its [`Entry`].
    fn remove_ordering_candidate(&mut self, id: transaction::Hash, time: Option<DateTime<Utc>>) {
        let entry = self
            .entries
            .remove(&id)
            .expect("every stored transaction has an entry");
        assert!(
            self.eviction_order
                .remove(&(entry.eviction, Reverse(time), id)),
            "every stored transaction has its current eviction candidate"
        );
        assert!(
            self.mining_order.remove(&(Reverse(entry.mining), time, id)),
            "every stored transaction has its current mining candidate"
        );
    }

    /// Adds both ordering entries for a transaction without an [`Entry`],
    /// and its entry with the stamp `added_at`.
    fn insert_ordering_candidate(&mut self, id: transaction::Hash, added_at: u64) {
        let eviction = self.eviction_candidate(id, &HashSet::new());
        let mining = self.mining_candidate(id);
        let (Reverse(mining_score), _, _) = mining;
        assert!(
            self.entries
                .insert(
                    id,
                    Entry {
                        eviction: eviction.0,
                        mining: mining_score,
                        added_at,
                    }
                )
                .is_none(),
            "a transaction's old keys were removed before reinsertion"
        );
        assert!(
            self.eviction_order.insert(eviction),
            "a transaction's old eviction candidate was removed before insertion"
        );
        assert!(
            self.mining_order.insert(mining),
            "a transaction's old mining candidate was removed before insertion"
        );
    }

    /// Refreshes surviving ancestors whose descendant packages changed.
    fn refresh_eviction_candidates(&mut self, ids: &HashSet<transaction::Hash>) {
        for &id in ids {
            let Some(tx) = self.transactions.get(&id) else {
                continue;
            };
            let time = tx.time;
            let score = self.package_score(&id, &HashSet::new());
            let cached = self
                .entries
                .get_mut(&id)
                .expect("every surviving transaction has an entry");
            if cached.eviction == score {
                continue;
            }
            assert!(
                self.eviction_order
                    .remove(&(cached.eviction, Reverse(time), id)),
                "the previous eviction candidate matches the cached score"
            );
            cached.eviction = score;
            assert!(
                self.eviction_order.insert((score, Reverse(time), id)),
                "the previous eviction candidate was removed before refresh"
            );
        }
    }

    /// Refreshes surviving descendants after their ancestors are mined.
    fn refresh_mining_candidates(&mut self, ids: &HashSet<transaction::Hash>) {
        for &id in ids {
            if !self.transactions.contains_key(&id) {
                continue;
            }
            let (Reverse(score), time, _) = self.mining_candidate(id);
            let cached = self
                .entries
                .get_mut(&id)
                .expect("every surviving transaction has an entry");
            if cached.mining == score {
                continue;
            }
            assert!(
                self.mining_order
                    .remove(&(Reverse(cached.mining), time, id)),
                "the previous mining candidate matches the cached score"
            );
            cached.mining = score;
            assert!(
                self.mining_order.insert((Reverse(score), time, id)),
                "the previous mining candidate was removed before refresh"
            );
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
        if !dependents.contains_key(&tx_id) {
            return HashSet::new();
        }
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

    /// Returns the unconfirmed ancestors of a stored transaction.
    fn transaction_ancestors(&self, id: transaction::Hash) -> HashSet<transaction::Hash> {
        self.ancestors(
            self.transaction_dependencies
                .dependencies()
                .get(&id)
                .into_iter()
                .flatten()
                .copied(),
        )
    }

    /// Removes the stored members of `mined_ids`, in that order, while
    /// retaining their unmined descendants. Returns what it removed, in that
    /// order too.
    ///
    /// Only surviving ancestors can change eviction priority. Only surviving
    /// descendants can change mining priority when their ancestors confirm.
    pub(super) fn remove_mined(&mut self, mined_ids: &[transaction::Hash]) -> Vec<UnminedTxId> {
        let mined: HashSet<_> = mined_ids
            .iter()
            .copied()
            .filter(|id| self.transactions.contains_key(id))
            .collect();
        let eviction_updates: HashSet<_> = mined
            .iter()
            .flat_map(|&id| self.transaction_ancestors(id))
            .collect();
        let mining_updates: HashSet<_> = mined
            .iter()
            .flat_map(|&id| self.descendants(id, &HashSet::new()))
            .collect();
        self.transaction_dependencies
            .clear_mined_dependencies(&mined);
        let removed = self.remove_roots(mined_ids.iter().copied());
        self.refresh_eviction_candidates(&eviction_updates);
        self.refresh_mining_candidates(&mining_updates);
        removed.into_iter().map(|tx| tx.transaction.id()).collect()
    }

    /// The stored members of `ids`, each once, oldest first.
    pub(super) fn oldest_first(
        &self,
        ids: impl IntoIterator<Item = transaction::Hash>,
    ) -> Vec<transaction::Hash> {
        let mut stamped: Vec<_> = ids
            .into_iter()
            .filter_map(|id| Some((self.added_at(&id)?, id)))
            .collect();
        stamped.sort_unstable();
        stamped.dedup();
        stamped.into_iter().map(|(_, id)| id).collect()
    }

    /// Removes `roots`, given oldest first, and what spends their outputs.
    /// Returns each removed transaction once, in the order the pool core
    /// logs them: each root followed by its stored spenders, depth first
    /// with children oldest first. A root goes as `reason`, unless an earlier
    /// root's removal took it, and its spenders as
    /// [`RemovalReason::Invalidated`]. Unstored roots are skipped.
    ///
    /// Spenders are found through the spend-owner map as well as the
    /// verifier's pool inputs, which name a subset of them. A child verified
    /// against a chain that had already mined its parent lists no pool
    /// input, yet its input is gone once the parent leaves unmined.
    pub(super) fn remove_with_descendants(
        &mut self,
        roots: &[transaction::Hash],
        reason: RemovalReason,
    ) -> Vec<(UnminedTxId, RemovalReason)> {
        let mut seen = HashSet::new();
        let mut removed = Vec::new();
        let mut report = Vec::new();
        for &root in roots {
            let mut pending = vec![(root, reason)];
            while let Some((id, reason)) = pending.pop() {
                let Some(tx) = self.transactions.get(&id) else {
                    continue;
                };
                if !seen.insert(id) {
                    continue;
                }
                report.push((tx.transaction.id(), reason));
                removed.push(id);
                let outputs = tx.transaction.transaction().outputs().len();
                let spenders = (0..outputs).filter_map(|index| {
                    self.spent_outpoints
                        .get(&transparent::OutPoint::from_usize(id, index))
                });
                let dependents = self
                    .transaction_dependencies
                    .dependents()
                    .get(&id)
                    .into_iter()
                    .flatten();
                let children = self.oldest_first(spenders.chain(dependents).copied());
                // Pushed newest first, so popped oldest first.
                pending.extend(
                    children
                        .into_iter()
                        .rev()
                        .map(|child| (child, RemovalReason::Invalidated)),
                );
            }
        }
        let batch = self.remove_batch(removed);
        debug_assert_eq!(
            batch.len(),
            report.len(),
            "the walk visits what the removal removes"
        );
        report
    }

    /// Removes each of `roots` in turn with what depends on it, as eviction
    /// selected them. Returns the removed ids in removal order, and the same
    /// ids oldest first.
    pub(super) fn evict(
        &mut self,
        roots: impl IntoIterator<Item = transaction::Hash>,
    ) -> (Vec<UnminedTxId>, Vec<UnminedTxId>) {
        let mut stamped = Vec::new();
        let mut removed = Vec::new();
        for root in roots {
            let stamps: HashMap<_, _> = self
                .descendants(root, &HashSet::new())
                .into_iter()
                .chain([root])
                .filter_map(|id| Some((id, self.added_at(&id)?)))
                .collect();
            for tx in self.remove_batch([root]) {
                let id = tx.transaction.id();
                stamped.push((stamps[&id.mined_id()], id));
                removed.push(id);
            }
        }
        stamped.sort_unstable_by_key(|(added_at, _)| *added_at);
        (removed, stamped.into_iter().map(|(_, id)| id).collect())
    }

    /// Removes roots and descendants, then refreshes their surviving ancestors.
    /// Survivors keep the same ancestors, so their mining priority is unchanged.
    pub(super) fn remove_batch(
        &mut self,
        tx_ids: impl IntoIterator<Item = transaction::Hash>,
    ) -> Vec<VerifiedUnminedTx> {
        let roots: Vec<_> = tx_ids
            .into_iter()
            .filter(|id| self.transactions.contains_key(id))
            .collect();
        let mut eviction_updates = HashSet::new();
        for &root in &roots {
            eviction_updates.extend(self.transaction_ancestors(root));
            // A removed descendant can have another, surviving parent, as in
            // a join. Its ancestors must also lose that descendant's fee.
            for descendant in self.descendants(root, &HashSet::new()) {
                eviction_updates.extend(self.transaction_ancestors(descendant));
            }
        }
        let removed = self.remove_roots(roots);
        self.refresh_eviction_candidates(&eviction_updates);
        removed
    }

    /// Removes each victim and its cached ordering entries.
    fn remove_roots(
        &mut self,
        tx_ids: impl IntoIterator<Item = transaction::Hash>,
    ) -> Vec<VerifiedUnminedTx> {
        let mut removed = Vec::new();
        for root in tx_ids {
            if !self.transactions.contains_key(&root) {
                continue;
            }
            let descendants = self.transaction_dependencies.remove_all(&root);
            for key_to_remove in descendants.into_iter().chain(std::iter::once(root)) {
                // The dependencies only name stored transactions, so this
                // skips nothing unless they are out of step.
                let Some(removed_tx) = self.transactions.remove(&key_to_remove) else {
                    continue;
                };

                self.remove_ordering_candidate(key_to_remove, removed_tx.time);

                self.transactions_serialized_size -= removed_tx.transaction.size();
                self.total_cost -= removed_tx.cost();
                self.weights.remove_transaction(&removed_tx);
                if let Some(height) = removed_tx.transaction.transaction().expiry_height() {
                    self.expiry_order.remove(&(height, key_to_remove));
                }
                self.remove_outputs(&removed_tx.transaction);

                removed.push(removed_tx);
            }
        }
        removed
    }

    /// Returns transactions expiring at or before `tip_height`.
    ///
    /// Iteration stops at the first future expiry, so unrelated transactions
    /// are never visited. Removal also removes any later-expiring descendants.
    pub(super) fn expired_transaction_ids(
        &self,
        tip_height: Height,
    ) -> impl Iterator<Item = transaction::Hash> + '_ {
        self.expiry_order
            .iter()
            .take_while(move |(height, _)| *height <= tip_height)
            .map(|(_, id)| *id)
    }

    /// Checks that every index matches the stored transactions, and the
    /// dependency-group bound.
    #[cfg(test)]
    pub(super) fn assert_dependency_groups_are_bounded(&self) {
        let removed = HashSet::new();
        let expected: HashMap<_, _> = self
            .transactions
            .keys()
            .map(|&id| {
                let (Reverse(mining), _, _) = self.mining_candidate(id);
                let added_at = self
                    .added_at(&id)
                    .expect("every stored transaction has an entry");
                assert!(
                    added_at <= self.insertions,
                    "a stamp is at most the insertion count"
                );
                (
                    id,
                    Entry {
                        eviction: self.eviction_candidate(id, &removed).0,
                        mining,
                        added_at,
                    },
                )
            })
            .collect();
        assert_eq!(self.entries, expected);
        let stamps: HashSet<_> = self.entries.values().map(|entry| entry.added_at).collect();
        assert_eq!(stamps.len(), self.entries.len(), "stamps are distinct");
        let expected: BTreeSet<_> = self
            .transactions
            .keys()
            .map(|&id| self.eviction_candidate(id, &removed))
            .collect();
        assert_eq!(self.eviction_order, expected);
        let expected: BTreeSet<_> = self
            .transactions
            .keys()
            .map(|&id| self.mining_candidate(id))
            .collect();
        assert_eq!(self.mining_order, expected);
        let expected: BTreeSet<_> = self
            .transactions
            .iter()
            .filter_map(|(&id, tx)| Some((tx.transaction.transaction().expiry_height()?, id)))
            .collect();
        assert_eq!(self.expiry_order, expected);
        // Each spend has one owner, so no two stored transactions conflict.
        let mut spent_outpoints = HashMap::new();
        let mut sapling_nullifiers = HashMap::new();
        let mut orchard_nullifiers = HashMap::new();
        let mut ironwood_nullifiers = HashMap::new();
        for (&id, tx) in &self.transactions {
            let tx = tx.transaction.transaction();
            for outpoint in tx.spent_outpoints() {
                assert!(spent_outpoints.insert(outpoint, id).is_none());
            }
            for &nullifier in tx.sapling_nullifiers() {
                assert!(sapling_nullifiers.insert(nullifier, id).is_none());
            }
            for &nullifier in tx.orchard_nullifiers() {
                assert!(orchard_nullifiers.insert(nullifier, id).is_none());
            }
            for &nullifier in tx.ironwood_nullifiers() {
                assert!(ironwood_nullifiers.insert(nullifier, id).is_none());
            }
        }
        assert_eq!(self.spent_outpoints, spent_outpoints);
        assert_eq!(self.sapling_nullifiers, sapling_nullifiers);
        assert_eq!(self.orchard_nullifiers, orchard_nullifiers);
        assert_eq!(self.ironwood_nullifiers, ironwood_nullifiers);
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
    pub fn has_spend_conflicts(&self, unmined_tx: &UnminedTx) -> bool {
        self.conflicting_ids(unmined_tx.transaction())
            .next()
            .is_some()
    }

    /// Finds owners of spends shared with `tx` using the admission indexes.
    ///
    /// A transaction can appear more than once if several spends conflict.
    /// Work scales with `tx`'s spends, independently of the mempool size.
    /// Sprout JoinSplits are excluded by mempool standardness policy.
    pub(super) fn conflicting_ids<'a>(
        &'a self,
        tx: &'a Transaction,
    ) -> impl Iterator<Item = transaction::Hash> + 'a {
        tx.spent_outpoints()
            .filter_map(|outpoint| self.spent_outpoints.get(&outpoint))
            .chain(
                tx.sapling_nullifiers()
                    .filter_map(|nf| self.sapling_nullifiers.get(nf)),
            )
            .chain(
                tx.orchard_nullifiers()
                    .filter_map(|nf| self.orchard_nullifiers.get(nf)),
            )
            .chain(
                tx.ironwood_nullifiers()
                    .filter_map(|nf| self.ironwood_nullifiers.get(nf)),
            )
            .copied()
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
        let sapling_nullifiers = tx.sapling_nullifiers().map(Cow::Borrowed);
        let orchard_nullifiers = tx.orchard_nullifiers().map(Cow::Borrowed);
        let ironwood_nullifiers = tx.ironwood_nullifiers().map(Cow::Borrowed);

        Self::remove_spends(&mut self.spent_outpoints, spent_outpoints);
        Self::remove_spends(&mut self.sapling_nullifiers, sapling_nullifiers);
        Self::remove_spends(&mut self.orchard_nullifiers, orchard_nullifiers);
        Self::remove_spends(&mut self.ironwood_nullifiers, ironwood_nullifiers);
    }

    /// Removes spends from an ownership index.
    ///
    /// Each item is wrapped in a [`Cow`] to support borrowed nullifiers and
    /// owned transparent outpoints.
    fn remove_spends<'t, T>(
        owners: &mut HashMap<T, transaction::Hash>,
        items: impl IntoIterator<Item = Cow<'t, T>>,
    ) where
        T: Clone + Eq + Hash + 't,
    {
        for item in items {
            owners.remove(&item);
        }
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

/// The sizes and actions of a set of transactions, by ZIP-317 fee weight
/// ratio, for the pool's metrics.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WeightTotals {
    /// Unpaid actions of transactions with a fee weight below 0.2.
    pub unpaid_actions_with_weight_lt20pct: u32,
    /// Unpaid actions of transactions with a fee weight from 0.2 to below 0.4.
    pub unpaid_actions_with_weight_lt40pct: u32,
    /// Unpaid actions of transactions with a fee weight from 0.4 to below 0.6.
    pub unpaid_actions_with_weight_lt60pct: u32,
    /// Unpaid actions of transactions with a fee weight from 0.6 to below 0.8.
    pub unpaid_actions_with_weight_lt80pct: u32,
    /// Unpaid actions of transactions with a fee weight from 0.8 to below 1.
    pub unpaid_actions_with_weight_lt1: u32,
    /// Paid actions of every transaction.
    pub paid_actions: u32,
    /// Serialized size of transactions with a fee weight below 1.
    pub size_with_weight_lt1: usize,
    /// Serialized size of transactions with a fee weight of exactly 1.
    pub size_with_weight_eq1: usize,
    /// Serialized size of transactions with a fee weight above 1, up to 2.
    pub size_with_weight_gt1: usize,
    /// Serialized size of transactions with a fee weight above 2, up to 3.
    pub size_with_weight_gt2: usize,
    /// Serialized size of transactions with a fee weight above 3.
    pub size_with_weight_gt3: usize,
}

impl WeightTotals {
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
        let mut totals = Self {
            paid_actions: conventional_actions - unpaid_actions,
            ..Default::default()
        };

        if fee_weight_ratio > 3.0 {
            totals.size_with_weight_gt3 = size;
        } else if fee_weight_ratio > 2.0 {
            totals.size_with_weight_gt2 = size;
        } else if fee_weight_ratio > 1.0 {
            totals.size_with_weight_gt1 = size;
        } else if fee_weight_ratio == 1.0 {
            totals.size_with_weight_eq1 = size;
        } else {
            totals.size_with_weight_lt1 = size;
            if fee_weight_ratio < 0.2 {
                totals.unpaid_actions_with_weight_lt20pct = unpaid_actions;
            } else if fee_weight_ratio < 0.4 {
                totals.unpaid_actions_with_weight_lt40pct = unpaid_actions;
            } else if fee_weight_ratio < 0.6 {
                totals.unpaid_actions_with_weight_lt60pct = unpaid_actions;
            } else if fee_weight_ratio < 0.8 {
                totals.unpaid_actions_with_weight_lt80pct = unpaid_actions;
            } else {
                totals.unpaid_actions_with_weight_lt1 = unpaid_actions;
            }
        }

        totals
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

    /// The node's default limit on one transaction's size, which bounds the
    /// cost of a newcomer and so how many victims it can need.
    const MAX_TRANSACTION_BYTES: u64 = 250_000;

    #[test]
    fn selection_rescores_only_changed_groups() {
        use std::sync::atomic::Ordering;

        use super::super::fixtures::TxFactory;

        let mut factory = TxFactory::new();
        let mut verified = VerifiedSet::default();
        for _ in 0..1_000 {
            let parent = factory.tx_with(10_000, 1, 0);
            let child = factory.tx_with(20_000, 1, 0);
            let grandchild = factory.tx(30_000);
            let output = |tx: &VerifiedUnminedTx| {
                transparent::OutPoint::from_usize(tx.transaction.id().mined_id(), 0)
            };
            verified
                .insert(parent.clone(), vec![], None, chrono::Utc::now())
                .unwrap();
            verified
                .insert(
                    child.clone(),
                    vec![output(&parent)],
                    None,
                    chrono::Utc::now(),
                )
                .unwrap();
            verified
                .insert(grandchild, vec![output(&child)], None, chrono::Utc::now())
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
        let max_victims =
            usize::try_from(MAX_TRANSACTION_BYTES.div_ceil(MEMPOOL_TRANSACTION_COST_THRESHOLD))
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
            transaction::Hash([0; 32]),
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
        let mut totals = WeightTotals::default();

        for &(fee_weight_ratio, conventional_actions, unpaid_actions, size) in &transaction_metrics
        {
            totals.add(WeightTotals::for_values(
                fee_weight_ratio,
                conventional_actions,
                unpaid_actions,
                size,
            ));
        }

        assert_eq!(
            totals,
            WeightTotals {
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
            totals.remove(WeightTotals::for_values(
                fee_weight_ratio,
                conventional_actions,
                unpaid_actions,
                size,
            ));
        }

        assert_eq!(totals, WeightTotals::default());
    }
}
