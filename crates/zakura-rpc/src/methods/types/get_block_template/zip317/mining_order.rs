//! Deterministic block template selection from the mempool's mining order.
//!
//! The mempool lists transactions by descending ancestor-package fee per cost.
//! Selection walks that list once, so a template costs work proportional to the
//! block it fills and the dependency groups it touches, not to the mempool.

use std::{
    cmp::Ordering,
    collections::{BTreeSet, HashMap, HashSet},
};

use zakura_chain::transaction::{self, VerifiedUnminedTx};
use zakura_node_services::mempool::TransactionDependencies;

#[cfg(test)]
use super::dependencies_depth;
use super::{BlockTemplateLimits, SelectedMempoolTx};

/// The priority pass stops once this few bytes remain in the template.
///
/// Smaller gaps rarely fit another transaction, so searching for one is wasted work.
pub(super) const SELECTION_STOP_BYTES: usize = 3_000;

/// The packing pass only runs when the priority pass leaves more than this many
/// bytes unused.
pub(super) const PACKING_THRESHOLD_BYTES: usize = 50_000;

/// The most mining-order entries the packing pass visits.
pub(super) const MAX_PACKING_SCAN_ENTRIES: usize = 100;

/// Unselected mempool transactions by mined ID.
pub(super) type Pool = HashMap<transaction::Hash, VerifiedUnminedTx>;

/// The mining priority of a transaction with its unselected ancestors.
///
/// Orders by descending package fee per ZIP-401 cost, then ascending insertion
/// time and transaction ID. This is the mempool's mining order, re-evaluated
/// after some ancestors have been selected. Comparisons cross-multiply in
/// `u128`, so they are exact.
#[derive(Clone, Copy, Debug)]
pub(super) struct PackagePriority {
    fee: u64,
    cost: u64,
    time: Option<chrono::DateTime<chrono::Utc>>,
    pub(super) id: transaction::Hash,
}

impl Ord for PackagePriority {
    fn cmp(&self, other: &Self) -> Ordering {
        (u128::from(other.fee) * u128::from(self.cost))
            .cmp(&(u128::from(self.fee) * u128::from(other.cost)))
            .then_with(|| self.time.cmp(&other.time))
            .then_with(|| self.id.cmp(&other.id))
    }
}

impl PartialOrd for PackagePriority {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for PackagePriority {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for PackagePriority {}

/// Returns `id` and its unselected ancestors, with parents before children.
///
/// Returns `None` if an unselected ancestor is missing from `pool`, because
/// the transaction then cannot be mined.
fn unselected_package(
    id: transaction::Hash,
    pool: &Pool,
    selected: &HashSet<transaction::Hash>,
    deps: &TransactionDependencies,
) -> Option<Vec<transaction::Hash>> {
    let dependencies = deps.dependencies();
    let mut package = Vec::new();
    let mut visited = HashSet::new();
    // A transaction is emitted when it is popped the second time, after its parents.
    let mut pending = vec![(id, false)];

    while let Some((id, parents_emitted)) = pending.pop() {
        if parents_emitted {
            package.push(id);
            continue;
        }
        if selected.contains(&id) || !visited.insert(id) {
            continue;
        }
        if !pool.contains_key(&id) {
            return None;
        }
        pending.push((id, true));
        pending.extend(
            dependencies
                .get(&id)
                .into_iter()
                .flatten()
                .map(|&parent| (parent, false)),
        );
    }

    Some(package)
}

/// Returns the priority of `id` with its unselected ancestors, or `None` if
/// one of them is missing from `pool`.
pub(super) fn package_priority(
    id: transaction::Hash,
    pool: &Pool,
    selected: &HashSet<transaction::Hash>,
    deps: &TransactionDependencies,
) -> Option<PackagePriority> {
    let mut fee = 0_u64;
    let mut cost = 0_u64;
    for member in unselected_package(id, pool, selected, deps)? {
        let member = &pool[&member];
        fee = fee.saturating_add(member.miner_fee.into());
        cost = cost.saturating_add(member.cost());
    }

    Some(PackagePriority {
        fee,
        cost,
        time: pool[&id].time,
        id,
    })
}

/// Selects template transactions from a mempool snapshot in mining order.
pub(super) struct MiningOrderSelector {
    /// The transactions that are not selected yet.
    pool: Pool,

    /// The snapshot's mining order, which scored every package before any
    /// transaction was selected.
    order: Vec<transaction::Hash>,

    /// The next unvisited entry in `order`.
    cursor: usize,

    deps: TransactionDependencies,
    limits: BlockTemplateLimits,

    /// The selected transactions, with parents before children.
    selected_txs: Vec<SelectedMempoolTx>,
    selected: HashSet<transaction::Hash>,

    /// Descendants of selected transactions. Their entries in `order` are
    /// stale, so the priority pass reads them from `rescored` instead.
    stale: HashSet<transaction::Hash>,

    /// The current priorities of the unselected `stale` transactions.
    rescored: BTreeSet<PackagePriority>,
    rescored_by_id: HashMap<transaction::Hash, PackagePriority>,

    /// The smallest transaction in the snapshot, in bytes.
    minimum_size: usize,
}

impl MiningOrderSelector {
    /// Returns a selector over `mempool_txs`, which the mempool supplies in
    /// mining order, for a template with the remaining capacity in `limits`.
    pub(super) fn new(
        mempool_txs: Vec<VerifiedUnminedTx>,
        deps: TransactionDependencies,
        limits: BlockTemplateLimits,
    ) -> Self {
        let mut pool = HashMap::with_capacity(mempool_txs.len());
        let mut order = Vec::with_capacity(mempool_txs.len());
        let mut minimum_size = usize::MAX;
        for tx in mempool_txs {
            let id = tx.transaction.id().mined_id();
            minimum_size = minimum_size.min(tx.transaction.size());
            order.push(id);
            pool.insert(id, tx);
        }

        Self {
            pool,
            order,
            cursor: 0,
            deps,
            limits,
            selected_txs: Vec::new(),
            selected: HashSet::new(),
            stale: HashSet::new(),
            rescored: BTreeSet::new(),
            rescored_by_id: HashMap::new(),
            minimum_size,
        }
    }

    /// Selects transactions in two passes and returns them with parents first.
    ///
    /// The priority pass adds the best remaining package until one does not fit
    /// or fewer than [`SELECTION_STOP_BYTES`] remain. If that leaves more than
    /// [`PACKING_THRESHOLD_BYTES`] unused, the packing pass adds the packages
    /// that fit from the next [`MAX_PACKING_SCAN_ENTRIES`] entries.
    pub(super) fn select(mut self) -> Vec<SelectedMempoolTx> {
        while self.limits.remaining_bytes > SELECTION_STOP_BYTES {
            let Some(id) = self.next_candidate() else {
                break;
            };
            if !self.try_add_package(id) {
                break;
            }
        }

        if self.limits.remaining_bytes > PACKING_THRESHOLD_BYTES {
            self.pack();
        }

        self.selected_txs
    }

    /// Removes and returns the best remaining candidate.
    ///
    /// Skips transactions that cannot be mined because an ancestor is missing.
    fn next_candidate(&mut self) -> Option<transaction::Hash> {
        // Entries that are not stale have no selected ancestors, so their
        // priorities are still the ones the snapshot was ordered by.
        let mut in_order = None;
        while let Some(&id) = self.order.get(self.cursor) {
            if !self.stale.contains(&id) && self.pool.contains_key(&id) {
                in_order = package_priority(id, &self.pool, &self.selected, &self.deps);
                if in_order.is_some() {
                    break;
                }
            }
            self.cursor += 1;
        }

        match (in_order, self.rescored.first()) {
            (Some(in_order), Some(rescored)) if in_order <= *rescored => {
                self.cursor += 1;
                Some(in_order.id)
            }
            (_, Some(_)) => {
                let rescored = self.rescored.pop_first()?;
                self.rescored_by_id.remove(&rescored.id);
                Some(rescored.id)
            }
            (Some(in_order), None) => {
                self.cursor += 1;
                Some(in_order.id)
            }
            (None, None) => None,
        }
    }

    /// Adds `id` and its unselected ancestors if they all fit, then rescores
    /// their remaining descendants. Otherwise changes nothing and returns `false`.
    fn try_add_package(&mut self, id: transaction::Hash) -> bool {
        let Some(package) = unselected_package(id, &self.pool, &self.selected, &self.deps) else {
            return false;
        };
        let mut limits = self.limits.clone();
        if !package.iter().all(|id| limits.try_add(&self.pool[id])) {
            return false;
        }
        self.limits = limits;
        for &id in &package {
            self.emit(id);
        }

        let dependents = self.deps.dependents();
        let mut descendants = HashSet::new();
        let mut pending = package;
        while let Some(id) = pending.pop() {
            for &child in dependents.get(&id).into_iter().flatten() {
                if descendants.insert(child) {
                    pending.push(child);
                }
            }
        }

        for id in descendants {
            if let Some(old) = self.rescored_by_id.remove(&id) {
                self.rescored.remove(&old);
            }
            self.stale.insert(id);
            if !self.pool.contains_key(&id) {
                continue;
            }
            if let Some(priority) = package_priority(id, &self.pool, &self.selected, &self.deps) {
                self.rescored.insert(priority);
                self.rescored_by_id.insert(id, priority);
            }
        }

        true
    }

    /// Fills the space left by the priority pass with later packages that fit.
    ///
    /// Continues from the priority pass's position in the mining order, so the
    /// selected prefix keeps its priority. Every visited entry counts towards
    /// [`MAX_PACKING_SCAN_ENTRIES`], whether or not it is added.
    fn pack(&mut self) {
        let scan_end = self
            .cursor
            .saturating_add(MAX_PACKING_SCAN_ENTRIES)
            .min(self.order.len());

        for position in self.cursor..scan_end {
            if self.limits.remaining_bytes <= SELECTION_STOP_BYTES
                || self.limits.remaining_bytes < self.minimum_size
            {
                break;
            }

            let id = self.order[position];
            if self.pool.contains_key(&id) {
                self.try_add_package(id);
            }
        }
    }

    /// Moves `id` from the pool to the end of the selected transactions.
    fn emit(&mut self, id: transaction::Hash) {
        let tx = self
            .pool
            .remove(&id)
            .expect("only unselected pool transactions are emitted");
        self.selected.insert(id);

        #[cfg(not(test))]
        self.selected_txs.push(tx);

        #[cfg(test)]
        self.selected_txs
            .push((dependencies_depth(&id, &self.deps), tx));
    }
}
