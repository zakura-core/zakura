//! Deterministic ancestor-package selection for block templates.
//!
//! Candidates arrive in the mempool's mining order: descending ancestor-package
//! fee per cost, then oldest insertion time, then TXID. Selecting a package
//! changes the scores of its surviving descendants, so those are rescored into
//! a small ordered set and merged with the remaining baseline order.

use std::{
    cmp::Ordering,
    collections::{BTreeSet, HashMap, HashSet},
};

use zakura_chain::transaction::{self, VerifiedUnminedTx};
use zakura_node_services::mempool::TransactionDependencies;

use super::{BlockTemplateLimits, SelectedMempoolTx};

/// The primary pass stops once this many bytes or fewer remain, because few
/// transactions fit in the gap and scanning for them is not worth the time.
pub(super) const SELECTION_STOP_BYTES: usize = 3_000;

/// Packing only runs when the primary pass leaves more than this many bytes.
pub(super) const PACKING_THRESHOLD_BYTES: usize = 50_000;

/// Packing visits at most this many baseline entries after the primary pass.
pub(super) const MAX_PACKING_SCAN_ENTRIES: usize = 100;

/// An ancestor-package score: highest fee per cost first, then the oldest
/// insertion time, then the lowest TXID.
///
/// This matches the mempool mining index, so the baseline order it supplies
/// is ascending in `Priority`.
#[derive(Clone, Copy, Debug)]
pub(super) struct Priority {
    fee: u64,
    cost: u64,
    time: Option<chrono::DateTime<chrono::Utc>>,
    pub(super) id: transaction::Hash,
}

impl Ord for Priority {
    fn cmp(&self, other: &Self) -> Ordering {
        (u128::from(other.fee) * u128::from(self.cost))
            .cmp(&(u128::from(self.fee) * u128::from(other.cost)))
            .then_with(|| self.time.cmp(&other.time))
            .then_with(|| self.id.cmp(&other.id))
    }
}

impl PartialOrd for Priority {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Priority {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Priority {}

/// Unselected candidate transactions by ID.
pub(super) type Pool = HashMap<transaction::Hash, VerifiedUnminedTx>;

/// Returns the direct mempool parents of `id`.
pub(super) fn parents<'a>(
    deps: &'a TransactionDependencies,
    id: &transaction::Hash,
) -> impl Iterator<Item = transaction::Hash> + 'a {
    deps.dependencies().get(id).into_iter().flatten().copied()
}

/// Returns `id` and its unselected ancestors in parent-first order, or `None`
/// if an ancestor is neither selected nor in `pool`.
///
/// Mempool admission bounds each dependency group to a few transactions, so
/// the quadratic ordering step is cheap. Ready members are taken in TXID
/// order so the result does not depend on hash iteration order.
fn package(
    id: transaction::Hash,
    pool: &Pool,
    selected: &HashMap<transaction::Hash, usize>,
    deps: &TransactionDependencies,
) -> Option<Vec<transaction::Hash>> {
    let mut pending = vec![id];
    let mut required = BTreeSet::new();
    while let Some(id) = pending.pop() {
        if selected.contains_key(&id) || !required.insert(id) {
            continue;
        }
        pool.get(&id)?;
        pending.extend(parents(deps, &id));
    }

    let mut ordered = Vec::with_capacity(required.len());
    while !required.is_empty() {
        let next = required.iter().copied().find(|id| {
            parents(deps, id)
                .all(|parent| selected.contains_key(&parent) || ordered.contains(&parent))
        })?;
        required.remove(&next);
        ordered.push(next);
    }
    Some(ordered)
}

/// Returns the score of `package`, the required package of its last member.
fn package_priority(package: &[transaction::Hash], pool: &Pool) -> Option<Priority> {
    let id = *package.last()?;
    let mut fee = 0_u64;
    let mut cost = 0_u64;
    for member in package {
        let member_fee: u64 = pool[member].miner_fee.into();
        fee = fee.checked_add(member_fee)?;
        cost = cost.checked_add(pool[member].cost())?;
    }
    Some(Priority {
        fee,
        cost,
        time: pool[&id].time,
        id,
    })
}

/// Returns the score of `id` with its unselected ancestors, or `None` if an
/// ancestor is unavailable.
#[cfg(test)]
pub(super) fn priority(
    id: transaction::Hash,
    pool: &Pool,
    selected: &HashMap<transaction::Hash, usize>,
    deps: &TransactionDependencies,
) -> Option<Priority> {
    package_priority(&package(id, pool, selected, deps)?, pool)
}

/// Selects ancestor packages in priority order until the template is full.
pub(super) struct IndexedSelector {
    pool: Pool,
    /// The baseline order, without rescoring for selected ancestors.
    order: Vec<transaction::Hash>,
    deps: TransactionDependencies,
    /// Dependency depth of each selected transaction within the template.
    selected_depths: HashMap<transaction::Hash, usize>,
    selected: Vec<SelectedMempoolTx>,
    /// Transactions whose baseline score is stale because an ancestor was selected.
    changed: HashSet<transaction::Hash>,
    /// Current scores of unselected `changed` transactions.
    updates: BTreeSet<Priority>,
    update_keys: HashMap<transaction::Hash, Priority>,
    cursor: usize,
    minimum_size: usize,
    limits: BlockTemplateLimits,
}

impl IndexedSelector {
    /// Creates a selector over `txs`, which should be in the mempool's mining
    /// order. Any order gives a deterministic result, but only the mining
    /// order gives the highest-priority packages first.
    pub(super) fn new(
        txs: Vec<VerifiedUnminedTx>,
        deps: TransactionDependencies,
        limits: BlockTemplateLimits,
    ) -> Self {
        let mut pool = HashMap::with_capacity(txs.len());
        let mut order = Vec::with_capacity(txs.len());
        let mut minimum_size = usize::MAX;
        for tx in txs {
            let id = tx.transaction.id().mined_id();
            minimum_size = minimum_size.min(tx.transaction.size());
            order.push(id);
            pool.insert(id, tx);
        }

        Self {
            pool,
            order,
            deps,
            selected_depths: HashMap::new(),
            selected: Vec::new(),
            changed: HashSet::new(),
            updates: BTreeSet::new(),
            update_keys: HashMap::new(),
            cursor: 0,
            minimum_size,
            limits,
        }
    }

    /// Runs the primary pass, then bounded packing, and returns the
    /// selected transactions, parents before children.
    pub(super) fn select(mut self) -> Vec<SelectedMempoolTx> {
        self.select_primary();
        self.pack();
        self.selected
    }

    /// Selects packages in priority order until the template is nearly full,
    /// the best remaining package does not fit, or no candidate remains.
    pub(super) fn select_primary(&mut self) {
        while self.limits.remaining_bytes > SELECTION_STOP_BYTES {
            let Some(package) = self.best() else {
                return;
            };
            if !self.add_package(package) {
                return;
            }
        }
    }

    /// Returns the transactions selected so far.
    #[cfg(test)]
    pub(super) fn selected(&self) -> &[SelectedMempoolTx] {
        &self.selected
    }

    /// Returns the required package of `id` and its score, or `None` if an
    /// ancestor is unavailable.
    fn candidate(&self, id: transaction::Hash) -> Option<(Vec<transaction::Hash>, Priority)> {
        let package = package(id, &self.pool, &self.selected_depths, &self.deps)?;
        let priority = package_priority(&package, &self.pool)?;
        Some((package, priority))
    }

    /// Removes and returns the highest-priority available package.
    #[allow(clippy::unwrap_in_result)]
    fn best(&mut self) -> Option<Vec<transaction::Hash>> {
        // Baseline entries that were selected, rescored, or have an
        // unavailable ancestor are passed over.
        let baseline = loop {
            let Some(&id) = self.order.get(self.cursor) else {
                break None;
            };
            if self.pool.contains_key(&id) && !self.changed.contains(&id) {
                if let Some(candidate) = self.candidate(id) {
                    break Some(candidate);
                }
            }
            self.cursor += 1;
        };

        let take_update = match (&baseline, self.updates.first()) {
            (Some((_, base)), Some(update)) => update < base,
            (None, Some(_)) => true,
            (_, None) => false,
        };

        if take_update {
            let update = self.updates.pop_first()?;
            self.update_keys.remove(&update.id);
            let package = package(update.id, &self.pool, &self.selected_depths, &self.deps)
                .expect("updates only hold candidates whose ancestors were available when scored, and selection never removes an unselected ancestor");
            return Some(package);
        }

        let (package, _) = baseline?;
        self.cursor += 1;
        Some(package)
    }

    /// Adds `package` if every member fits, then rescores its descendants.
    /// Returns `false` and leaves the template unchanged if it does not fit.
    fn add_package(&mut self, package: Vec<transaction::Hash>) -> bool {
        let mut limits = self.limits.clone();
        if !package.iter().all(|id| limits.try_add(&self.pool[id])) {
            return false;
        }
        self.limits = limits;

        for &id in &package {
            self.emit(id);
        }

        let mut pending = package;
        let mut affected = HashSet::new();
        while let Some(id) = pending.pop() {
            for &child in self.deps.dependents().get(&id).into_iter().flatten() {
                if affected.insert(child) {
                    pending.push(child);
                }
            }
        }

        for id in affected {
            self.changed.insert(id);
            if self.pool.contains_key(&id) {
                self.rescore(id);
            }
        }

        true
    }

    /// Replaces the update entry for `id` with its current score.
    fn rescore(&mut self, id: transaction::Hash) {
        if let Some(old) = self.update_keys.remove(&id) {
            self.updates.remove(&old);
        }
        if let Some((_, priority)) = self.candidate(id) {
            self.updates.insert(priority);
            self.update_keys.insert(id, priority);
        }
    }

    /// Moves `id` from the pool to the template. Its parents must already be
    /// selected.
    fn emit(&mut self, id: transaction::Hash) {
        let depth = parents(&self.deps, &id)
            .map(|parent| self.selected_depths[&parent] + 1)
            .max()
            .unwrap_or(0);
        self.selected_depths.insert(id, depth);

        if let Some(old) = self.update_keys.remove(&id) {
            self.updates.remove(&old);
        }

        let tx = self
            .pool
            .remove(&id)
            .expect("emitted transactions come from the unselected pool");

        #[cfg(test)]
        self.selected.push((depth, tx));

        #[cfg(not(test))]
        self.selected.push(tx);
    }

    /// Fills a large remaining gap with transactions from the next few
    /// baseline entries whose parents are already selected. Returns the
    /// number of entries visited.
    pub(super) fn pack(&mut self) -> usize {
        if self.limits.remaining_bytes <= PACKING_THRESHOLD_BYTES {
            return 0;
        }

        let mut scanned = 0;
        let scan_end = self
            .cursor
            .saturating_add(MAX_PACKING_SCAN_ENTRIES)
            .min(self.order.len());

        // Every visited entry counts, including selected or ineligible ones.
        for offset in self.cursor..scan_end {
            if self.limits.remaining_bytes <= SELECTION_STOP_BYTES
                || self.limits.remaining_bytes < self.minimum_size
            {
                break;
            }
            scanned += 1;

            let id = self.order[offset];
            let Some(tx) = self.pool.get(&id) else {
                continue;
            };
            if parents(&self.deps, &id).all(|parent| self.selected_depths.contains_key(&parent))
                && self.limits.try_add(tx)
            {
                self.emit(id);
            }
        }

        scanned
    }
}
