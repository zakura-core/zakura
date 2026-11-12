//! Test-only selection variants, for comparing against the production
//! selector in the GBT benchmark.
//!
//! Each variant mirrors [`super::indexed::IndexedSelector`] (ancestor
//! packages, descendant rescoring, merge of baseline order and updates), but
//! changes the package score or what happens when a package does not fit.

use std::{
    cmp::Ordering,
    collections::{BTreeSet, HashMap, HashSet},
};

use zakura_chain::{
    block::MAX_BLOCK_BYTES,
    transaction::{self, VerifiedUnminedTx},
};
use zakura_consensus::MAX_BLOCK_SIGOPS;
use zakura_node_services::mempool::TransactionDependencies;

use super::{
    indexed::{package, parents, Pool, SELECTION_STOP_BYTES},
    BlockTemplateLimits, SelectedMempoolTx,
};

/// Block bytes per block sigop, so a transaction's sigops can be priced in
/// the same unit as its size.
const BYTES_PER_SIGOP: u64 = MAX_BLOCK_BYTES / MAX_BLOCK_SIGOPS as u64;

/// What a package's fee is divided by.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Score {
    /// ZIP-401 cost: serialized size with a 10,000 floor. Matches the
    /// mempool mining index and the production selector.
    Zip401Cost,
    /// Serialized size, without the floor.
    Size,
    /// The larger of serialized size and sigops priced at the block's
    /// bytes-per-sigop ratio.
    SigopAdjustedSize,
}

impl Score {
    fn weight(self, tx: &VerifiedUnminedTx) -> u64 {
        let size = u64::try_from(tx.transaction.size()).expect("transaction sizes fit in u64");
        match self {
            Self::Zip401Cost => tx.cost(),
            Self::Size => size,
            Self::SigopAdjustedSize => {
                size.max(u64::from(tx.block_sigop_count()) * BYTES_PER_SIGOP)
            }
        }
    }
}

/// What the main pass does when the best package does not fit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Misfit {
    /// End the main pass, like the production selector.
    Stop,
    /// Skip the package and keep going until the template is nearly full or
    /// candidates run out.
    Skip,
}

/// A package score under a [`Score`]: highest fee per weight first, then the
/// oldest insertion time, then the lowest TXID.
#[derive(Clone, Copy, Debug)]
struct Key {
    fee: u64,
    weight: u64,
    time: Option<chrono::DateTime<chrono::Utc>>,
    id: transaction::Hash,
}

impl Ord for Key {
    fn cmp(&self, other: &Self) -> Ordering {
        (u128::from(other.fee) * u128::from(self.weight))
            .cmp(&(u128::from(self.fee) * u128::from(other.weight)))
            .then_with(|| self.time.cmp(&other.time))
            .then_with(|| self.id.cmp(&other.id))
    }
}

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Key {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Key {}

/// The result of a variant selection.
pub(super) struct VariantSelection {
    pub(super) txs: Vec<SelectedMempoolTx>,
    /// Packages that did not fit when they were the best candidate.
    pub(super) misfits: usize,
}

/// A selector with a configurable score and misfit rule.
pub(super) struct VariantSelector {
    pool: Pool,
    order: Vec<transaction::Hash>,
    deps: TransactionDependencies,
    selected_depths: HashMap<transaction::Hash, usize>,
    selected: Vec<SelectedMempoolTx>,
    changed: HashSet<transaction::Hash>,
    updates: BTreeSet<Key>,
    update_keys: HashMap<transaction::Hash, Key>,
    cursor: usize,
    limits: BlockTemplateLimits,
    score: Score,
    misfit: Misfit,
    misfits: usize,
}

impl VariantSelector {
    /// Creates a selector over `txs` in mempool mining order. Scores other
    /// than [`Score::Zip401Cost`] disagree with that order, so the selector
    /// sorts its own baseline for them, as production would have to.
    pub(super) fn new(
        txs: Vec<VerifiedUnminedTx>,
        deps: TransactionDependencies,
        limits: BlockTemplateLimits,
        score: Score,
        misfit: Misfit,
    ) -> Self {
        let mut pool = HashMap::with_capacity(txs.len());
        let mut order = Vec::with_capacity(txs.len());
        for tx in txs {
            let id = tx.transaction.id().mined_id();
            order.push(id);
            pool.insert(id, tx);
        }

        let mut selector = Self {
            pool,
            order,
            deps,
            selected_depths: HashMap::new(),
            selected: Vec::new(),
            changed: HashSet::new(),
            updates: BTreeSet::new(),
            update_keys: HashMap::new(),
            cursor: 0,
            limits,
            score,
            misfit,
            misfits: 0,
        };

        if score != Score::Zip401Cost {
            let mut keyed: Vec<_> = selector
                .order
                .iter()
                .map(|&id| (selector.candidate(id).map(|(_, key)| key), id))
                .collect();
            // Candidates with unavailable ancestors sort last and are skipped
            // by the scan.
            keyed.sort_unstable_by(|(left, left_id), (right, right_id)| match (left, right) {
                (Some(left), Some(right)) => left.cmp(right),
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (None, None) => left_id.cmp(right_id),
            });
            selector.order = keyed.into_iter().map(|(_, id)| id).collect();
        }

        selector
    }

    /// Runs the main pass. There is no separate packing pass.
    pub(super) fn select(mut self) -> VariantSelection {
        while self.limits.remaining_bytes > SELECTION_STOP_BYTES {
            let Some(package) = self.best() else {
                break;
            };
            if !self.add_package(package) {
                self.misfits += 1;
                if self.misfit == Misfit::Stop {
                    break;
                }
            }
        }

        VariantSelection {
            txs: self.selected,
            misfits: self.misfits,
        }
    }

    fn candidate(&self, id: transaction::Hash) -> Option<(Vec<transaction::Hash>, Key)> {
        let package = package(id, &self.pool, &self.selected_depths, &self.deps)?;
        let mut fee = 0_u64;
        let mut weight = 0_u64;
        for member in &package {
            let tx = &self.pool[member];
            let member_fee: u64 = tx.miner_fee.into();
            fee = fee.checked_add(member_fee)?;
            weight = weight.checked_add(self.score.weight(tx))?;
        }
        let key = Key {
            fee,
            weight,
            time: self.pool[&id].time,
            id,
        };
        Some((package, key))
    }

    fn best(&mut self) -> Option<Vec<transaction::Hash>> {
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
            return package(update.id, &self.pool, &self.selected_depths, &self.deps);
        }

        let (package, _) = baseline?;
        self.cursor += 1;
        Some(package)
    }

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

    fn rescore(&mut self, id: transaction::Hash) {
        if let Some(old) = self.update_keys.remove(&id) {
            self.updates.remove(&old);
        }
        if let Some((_, key)) = self.candidate(id) {
            self.updates.insert(key);
            self.update_keys.insert(id, key);
        }
    }

    fn emit(&mut self, id: transaction::Hash) {
        let depth = parents(&self.deps, &id)
            .map(|parent| self.selected_depths[&parent] + 1)
            .max()
            .unwrap_or(0);
        self.selected_depths.insert(id, depth);
        if let Some(old) = self.update_keys.remove(&id) {
            self.updates.remove(&old);
        }
        let tx = self.pool.remove(&id).expect("emitted from the pool");
        self.selected.push((depth, tx));
    }
}
