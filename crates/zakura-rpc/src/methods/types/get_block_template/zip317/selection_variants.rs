//! Test-only selection variants, for comparing against the production
//! selector in the GBT benchmark.
//!
//! [`VariantSelector`] mirrors [`super::indexed::IndexedSelector`] (ancestor
//! packages, descendant rescoring, merge of baseline order and updates), but
//! changes the package score or what happens when a package does not fit.
//! [`weighted_random`] is the selector that preceded the indexed one.

use std::{
    cmp::Ordering,
    collections::{BTreeSet, HashMap, HashSet},
};

use rand_10::{
    distr::{Distribution, Open01},
    Rng,
};
use zakura_chain::{
    block::MAX_BLOCK_BYTES,
    transaction::{self, VerifiedUnminedTx},
};
use zakura_node_services::mempool::TransactionDependencies;

use super::{
    indexed::{
        package, parents, Pool, MAX_PACKING_SCAN_ENTRIES, PACKING_THRESHOLD_BYTES,
        SELECTION_STOP_BYTES,
    },
    BlockTemplateLimits, SelectedMempoolTx,
};

/// Once the template is within this many bytes of full, [`Misfit::Skip`]
/// gives up after [`MAX_CONSECUTIVE_MISFITS`] packages in a row do not fit.
const NEARLY_FULL_BYTES: usize = 50_000;

/// Consecutive misfits [`Misfit::Skip`] tolerates in a nearly full template.
/// Matches Bitcoin Core's `MAX_CONSECUTIVE_FAILURES`.
const MAX_CONSECUTIVE_MISFITS: usize = 1_000;

/// What a package's fee is divided by.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Score {
    /// ZIP-401 cost: serialized size with a 10,000 floor. Matches the
    /// mempool mining index and the production selector.
    Zip401Cost,
    /// The larger of serialized size and sigops priced at the block's
    /// bytes-per-sigop ratio.
    SigopAdjustedSize,
    /// The largest share of any block limit the transaction uses (bytes,
    /// sigops, and the ZIP 218 shielded limits when active), scaled to bytes.
    ResourceAdjustedSize,
}

/// What the main pass does when the best package does not fit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Misfit {
    /// End the main pass, then pack from the next entries, like the
    /// production selector.
    StopThenPack,
    /// Skip the package and keep going, until the template is nearly full
    /// and [`MAX_CONSECUTIVE_MISFITS`] packages in a row did not fit.
    Skip,
}

/// Block capacity after the coinbase, for pricing each limit in bytes.
#[derive(Clone, Copy)]
struct Capacity {
    bytes: f64,
    sigops: f64,
    /// `None` while the ZIP 218 limits are inactive.
    shielded: Option<[f64; 5]>,
}

impl Capacity {
    fn new(limits: &BlockTemplateLimits) -> Self {
        let shielded = (limits.remaining_shielded_cost != u32::MAX).then(|| {
            [
                limits.remaining_orchard_actions,
                limits.remaining_ironwood_actions,
                limits.remaining_sapling_ios,
                limits.remaining_sprout_joinsplits,
                limits.remaining_shielded_cost,
            ]
            .map(f64::from)
        });
        Self {
            bytes: limits.remaining_bytes as f64,
            sigops: f64::from(limits.remaining_sigops),
            shielded,
        }
    }
}

impl Score {
    fn weight(self, tx: &VerifiedUnminedTx, capacity: &Capacity) -> u64 {
        let size = u64::try_from(tx.transaction.size()).expect("transaction sizes fit in u64");
        match self {
            Self::Zip401Cost => tx.cost(),
            Self::SigopAdjustedSize => {
                let bytes_per_sigop =
                    MAX_BLOCK_BYTES / u64::from(zakura_consensus::MAX_BLOCK_SIGOPS);
                size.max(u64::from(tx.block_sigop_count()) * bytes_per_sigop)
            }
            Self::ResourceAdjustedSize => {
                let mut share = size as f64 / capacity.bytes;
                share = share.max(f64::from(tx.block_sigop_count()) / capacity.sigops);
                if let Some(shielded) = capacity.shielded {
                    let counts = tx.transaction.transaction().shielded_action_counts();
                    let used = [
                        counts.orchard_actions,
                        counts.ironwood_actions,
                        counts.sapling_ios,
                        counts.sprout_joinsplits,
                        counts.cost(),
                    ];
                    for (used, capacity) in used.into_iter().zip(shielded) {
                        if capacity > 0.0 {
                            share = share.max(f64::from(used) / capacity);
                        }
                    }
                }
                // At least one byte, so every weight is positive.
                ((share * capacity.bytes).ceil() as u64).max(1)
            }
        }
    }
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
    capacity: Capacity,
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

        let capacity = Capacity::new(&limits);
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
            capacity,
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

    /// Runs the main pass, and packing for [`Misfit::StopThenPack`].
    pub(super) fn select(mut self) -> VariantSelection {
        let mut consecutive_misfits = 0;
        while self.limits.remaining_bytes > SELECTION_STOP_BYTES {
            let Some(package) = self.best() else {
                break;
            };
            if self.add_package(package) {
                consecutive_misfits = 0;
                continue;
            }
            self.misfits += 1;
            consecutive_misfits += 1;
            match self.misfit {
                Misfit::StopThenPack => break,
                Misfit::Skip => {
                    if consecutive_misfits >= MAX_CONSECUTIVE_MISFITS
                        && self.limits.remaining_bytes <= NEARLY_FULL_BYTES
                    {
                        break;
                    }
                }
            }
        }

        if self.misfit == Misfit::StopThenPack {
            self.pack();
        }

        VariantSelection {
            txs: self.selected,
            misfits: self.misfits,
        }
    }

    /// Mirrors the production packing pass.
    fn pack(&mut self) {
        if self.limits.remaining_bytes <= PACKING_THRESHOLD_BYTES {
            return;
        }
        let minimum_size = self
            .pool
            .values()
            .map(|tx| tx.transaction.size())
            .min()
            .unwrap_or(usize::MAX);
        let scan_end = self
            .cursor
            .saturating_add(MAX_PACKING_SCAN_ENTRIES)
            .min(self.order.len());
        for offset in self.cursor..scan_end {
            if self.limits.remaining_bytes <= SELECTION_STOP_BYTES
                || self.limits.remaining_bytes < minimum_size
            {
                break;
            }
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
    }

    fn candidate(&self, id: transaction::Hash) -> Option<(Vec<transaction::Hash>, Key)> {
        let package = package(id, &self.pool, &self.selected_depths, &self.deps)?;
        let mut fee = 0_u64;
        let mut weight = 0_u64;
        for member in &package {
            let tx = &self.pool[member];
            let member_fee: u64 = tx.miner_fee.into();
            fee = fee.checked_add(member_fee)?;
            weight = weight.checked_add(self.score.weight(tx, &self.capacity))?;
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

/// The weighted-random selector from main before this change: conventional-fee
/// independent transactions in fee-weighted random order, each followed by
/// dependents whose dependencies are all selected, then low-fee ones.
pub(super) fn weighted_random(
    txs: Vec<VerifiedUnminedTx>,
    deps: &TransactionDependencies,
    mut limits: BlockTemplateLimits,
    rng: &mut impl Rng,
) -> Vec<SelectedMempoolTx> {
    let dependencies = deps.dependencies();
    let (independent, mut dependent): (HashMap<_, _>, HashMap<_, _>) = txs
        .into_iter()
        .map(|tx| (tx.transaction.id().mined_id(), tx))
        .partition(|(id, _)| !dependencies.contains_key(id));
    let (conventional, low_fee): (Vec<_>, Vec<_>) = independent
        .into_values()
        .partition(VerifiedUnminedTx::pays_conventional_fee);

    let mut selected: Vec<SelectedMempoolTx> = Vec::new();
    let mut selected_ids = HashSet::new();
    for candidates in [conventional, low_fee] {
        let mut clocks: Vec<_> = candidates
            .into_iter()
            .map(|tx| {
                let uniform: f64 = Open01.sample(rng);
                (-uniform.ln() / f64::from(tx.fee_weight_ratio), tx)
            })
            .collect();
        clocks.sort_unstable_by(|(left, _), (right, _)| left.total_cmp(right));

        for (_, candidate) in clocks {
            if !limits.try_add(&candidate) {
                continue;
            }
            let id = candidate.transaction.id().mined_id();
            selected_ids.insert(id);
            selected.push((0, candidate));

            let mut level = deps.direct_dependents(&id);
            while !level.is_empty() {
                let mut next = HashSet::new();
                for dependent_id in &level {
                    let ready = dependencies
                        .get(dependent_id)
                        .is_none_or(|parents| parents.iter().all(|p| selected_ids.contains(p)));
                    if !ready {
                        continue;
                    }
                    let Some(tx) = dependent.remove(dependent_id) else {
                        continue;
                    };
                    if !tx.pays_conventional_fee() || !limits.try_add(&tx) {
                        continue;
                    }
                    selected_ids.insert(*dependent_id);
                    selected.push((1, tx));
                    next.extend(deps.direct_dependents(dependent_id));
                }
                level = next;
            }
        }
    }
    selected
}
