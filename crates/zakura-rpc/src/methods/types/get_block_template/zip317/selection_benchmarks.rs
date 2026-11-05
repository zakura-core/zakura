//! Test-only indexed selector prototype and GBT CPU benchmarks.
//!
//! Production selection stays unchanged. Fixtures have canonical encoded shapes
//! and unique nullifiers, but placeholder proofs/signatures are not verified.

use std::{
    cmp::Ordering,
    collections::BTreeSet,
    hint::black_box,
    sync::Arc,
    time::{Duration, Instant},
};

use proptest::{prelude::*, strategy::ValueTree, test_runner::TestRunner};
use zcash_transparent::address::TransparentAddress;

use zakura_chain::{
    amount::NonNegative,
    block::{self, CHAIN_HISTORY_ACTIVATION_RESERVED},
    ironwood,
    primitives::Halo2Proof,
    serialization::{DateTime32, ZcashSerialize},
    transaction::{LockTime, Transaction},
    transparent::{Input, OutPoint, Output, Script},
    value_balance::ValueBalance,
    work::difficulty::{CompactDifficulty, ExpandedDifficulty, U256},
};
use zakura_state::GetBlockTemplateChainInfo;

use super::*;
use crate::methods::types::get_block_template::{
    proposal::proposal_block_from_template, BlockTemplateResponse, MinerAddress,
};

const PACKING_THRESHOLD_BYTES: usize = 50_000;
const SAMPLES: usize = 12;

#[derive(Clone, Copy, Debug)]
struct Priority {
    fee: u64,
    cost: u64,
    time: Option<chrono::DateTime<chrono::Utc>>,
    id: transaction::Hash,
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

type Pool = HashMap<transaction::Hash, VerifiedUnminedTx>;

fn parents<'a>(
    deps: &'a TransactionDependencies,
    id: &transaction::Hash,
) -> impl Iterator<Item = transaction::Hash> + 'a {
    deps.dependencies().get(id).into_iter().flatten().copied()
}

/// Returns the still-required package in parent-first order, rejecting missing
/// ancestors or cycles. Only the fixture's bounded dependency groups are used.
fn package(
    id: transaction::Hash,
    pool: &Pool,
    selected: &HashMap<transaction::Hash, usize>,
    deps: &TransactionDependencies,
) -> Option<Vec<transaction::Hash>> {
    let mut pending = vec![id];
    let mut required = HashSet::new();
    while let Some(id) = pending.pop() {
        if selected.contains_key(&id) || !required.insert(id) {
            continue;
        }
        pool.get(&id)?;
        pending.extend(parents(deps, &id));
    }
    let mut ordered = Vec::new();
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

fn priority(
    id: transaction::Hash,
    pool: &Pool,
    selected: &HashMap<transaction::Hash, usize>,
    deps: &TransactionDependencies,
) -> Option<Priority> {
    let mut fee = 0_u64;
    let mut cost = 0_u64;
    for member in package(id, pool, selected, deps)? {
        let member_fee: u64 = pool[&member].miner_fee.into();
        fee = fee.checked_add(member_fee)?;
        cost = cost.checked_add(pool[&member].cost())?;
    }
    Some(Priority {
        fee,
        cost,
        time: pool[&id].time,
        id,
    })
}

fn limits(network: &Network, height: Height, miner: &MinerParams) -> BlockTemplateLimits {
    let coinbase =
        TransactionTemplate::coinbase_resource_usage(network, height, miner, None).unwrap();
    BlockTemplateLimits::initial(network, height, coinbase)
}

fn copy_limits(value: &BlockTemplateLimits) -> BlockTemplateLimits {
    BlockTemplateLimits {
        remaining_bytes: value.remaining_bytes,
        remaining_sigops: value.remaining_sigops,
        remaining_unpaid_actions: value.remaining_unpaid_actions,
        remaining_orchard_actions: value.remaining_orchard_actions,
        remaining_ironwood_actions: value.remaining_ironwood_actions,
        remaining_sapling_ios: value.remaining_sapling_ios,
        remaining_sprout_joinsplits: value.remaining_sprout_joinsplits,
        remaining_shielded_cost: value.remaining_shielded_cost,
    }
}

struct IndexedSelector {
    pool: Pool,
    order: Vec<transaction::Hash>,
    deps: TransactionDependencies,
    selected_depths: HashMap<transaction::Hash, usize>,
    selected: Vec<SelectedMempoolTx>,
    changed: HashSet<transaction::Hash>,
    updates: BTreeSet<Priority>,
    update_keys: HashMap<transaction::Hash, Priority>,
    cursor: usize,
    minimum_size: usize,
    limits: BlockTemplateLimits,
}

impl IndexedSelector {
    fn new(
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

    fn best(&mut self) -> Option<transaction::Hash> {
        while self.cursor < self.order.len()
            && (self.changed.contains(&self.order[self.cursor])
                || !self.pool.contains_key(&self.order[self.cursor]))
        {
            self.cursor += 1;
        }
        let baseline = self
            .order
            .get(self.cursor)
            .and_then(|id| priority(*id, &self.pool, &self.selected_depths, &self.deps));
        let updated = self.updates.first().copied();
        match (baseline, updated) {
            (Some(base), Some(local)) if base <= local => {
                self.cursor += 1;
                Some(base.id)
            }
            (_, Some(_)) => {
                let next = self.updates.pop_first()?;
                self.update_keys.remove(&next.id);
                Some(next.id)
            }
            (Some(base), None) => {
                self.cursor += 1;
                Some(base.id)
            }
            (None, None) => None,
        }
    }

    fn emit(&mut self, id: transaction::Hash) {
        let depth = parents(&self.deps, &id)
            .map(|parent| self.selected_depths[&parent] + 1)
            .max()
            .unwrap_or(0);
        self.selected_depths.insert(id, depth);
        self.selected.push((depth, self.pool.remove(&id).unwrap()));
    }

    fn add_package(&mut self, id: transaction::Hash) -> bool {
        let Some(package) = package(id, &self.pool, &self.selected_depths, &self.deps) else {
            return false;
        };
        let mut next_limits = copy_limits(&self.limits);
        if !package.iter().all(|id| next_limits.try_add(&self.pool[id])) {
            return false;
        }
        self.limits = next_limits;
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
            if let Some(old) = self.update_keys.remove(&id) {
                self.updates.remove(&old);
            }
            self.changed.insert(id);
            if self.pool.contains_key(&id) {
                let next = priority(id, &self.pool, &self.selected_depths, &self.deps).unwrap();
                self.updates.insert(next);
                self.update_keys.insert(id, next);
            }
        }
        true
    }

    fn finish(mut self, pack: bool) -> Selection {
        while let Some(id) = self.best() {
            if !self.add_package(id) {
                break;
            }
        }
        let mut scanned = 0;
        let mut packing_elapsed = Duration::ZERO;
        if pack && self.limits.remaining_bytes > PACKING_THRESHOLD_BYTES {
            let start = Instant::now();
            for offset in 0..self.order.len() {
                if self.limits.remaining_bytes < self.minimum_size {
                    break;
                }
                scanned += 1;
                let id = self.order[offset];
                let Some(tx) = self.pool.get(&id) else {
                    continue;
                };
                if tx.transaction.size() <= self.limits.remaining_bytes
                    && parents(&self.deps, &id)
                        .all(|parent| self.selected_depths.contains_key(&parent))
                    && self.limits.try_add(tx)
                {
                    self.emit(id);
                }
            }
            packing_elapsed = start.elapsed();
        }
        Selection {
            txs: self.selected,
            scanned,
            packing_elapsed,
        }
    }
}

struct Factory {
    action: ironwood::Action,
    serial: u64,
}
impl Factory {
    fn new() -> Self {
        Self {
            action: any::<ironwood::Action>()
                .new_tree(&mut TestRunner::deterministic())
                .unwrap()
                .current(),
            serial: 0,
        }
    }
    fn tx(
        &mut self,
        actions: usize,
        fee_multiple: u64,
        spent: Vec<OutPoint>,
        outputs: usize,
    ) -> VerifiedUnminedTx {
        let mut authorized = Vec::new();
        for _ in 0..actions {
            self.serial += 1;
            let mut action = self.action.clone();
            let mut bytes = [0; 32];
            bytes[..8].copy_from_slice(&self.serial.to_le_bytes());
            action.nullifier = bytes
                .try_into()
                .expect("a u64 is below the Pallas field modulus");
            authorized.push(ironwood::AuthorizedAction {
                action,
                spend_auth_sig: [0; 64].into(),
            });
        }
        let tx = Transaction::V6 {
            network_upgrade: NetworkUpgrade::Nu6_3,
            lock_time: LockTime::unlocked(),
            expiry_height: Height(0),
            inputs: spent
                .into_iter()
                .map(|outpoint| Input::PrevOut {
                    outpoint,
                    unlock_script: Script::new(&[]),
                    sequence: u32::MAX,
                })
                .collect(),
            outputs: (0..outputs)
                .map(|_| {
                    Output::new(
                        Amount::new(1_000_000),
                        Script::new(&[
                            0x76, 0xa9, 0x14, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                            0, 0, 0x88, 0xac,
                        ]),
                    )
                })
                .collect(),
            sapling_shielded_data: None,
            orchard_shielded_data: None,
            ironwood_shielded_data: Some(ironwood::ShieldedData {
                flags: ironwood::Flags::ENABLE_SPENDS,
                value_balance: Amount::zero(),
                shared_anchor: Default::default(),
                proof: Halo2Proof(vec![0; ::orchard::Proof::expected_proof_size(actions)]),
                actions: authorized.try_into().unwrap(),
                binding_sig: [0; 64].into(),
            }),
        };
        assert!(tx
            .ironwood_shielded_data()
            .unwrap()
            .proof_size_is_canonical());
        let unmined: transaction::UnminedTx = tx.into();
        let conventional: u64 = unmined.conventional_fee().into();
        let mut verified = VerifiedUnminedTx::new(
            unmined,
            Amount::try_from(conventional * fee_multiple).unwrap(),
            u32::try_from(outputs).unwrap(),
            0,
            Arc::new(Vec::new()),
        )
        .unwrap();
        verified.time =
            Some(chrono::DateTime::from_timestamp(i64::try_from(self.serial).unwrap(), 0).unwrap());
        verified
    }
}

struct Fixture {
    pool: Pool,
    index: BTreeSet<Priority>,
    deps: TransactionDependencies,
    cost: u64,
}
impl Fixture {
    fn from_transactions(txs: Vec<VerifiedUnminedTx>) -> Self {
        let mut deps = TransactionDependencies::default();
        let mut pool = HashMap::new();
        let mut cost = 0;
        for tx in txs {
            let id = tx.transaction.id().mined_id();
            deps.add(id, tx.transaction.transaction().spent_outpoints().collect());
            cost += tx.cost();
            pool.insert(id, tx);
        }
        let selected = HashMap::new();
        let index = pool
            .keys()
            .map(|id| priority(*id, &pool, &selected, &deps).unwrap())
            .collect();
        Self {
            pool,
            index,
            deps,
            cost,
        }
    }
    fn full(kind: &str, cost_limit: u64) -> Self {
        let mut factory = Factory::new();
        let mut txs = Vec::new();
        let mut cost = 0;
        if matches!(kind, "mixed" | "mixed_packages") {
            for _ in 0..32 {
                let tx = factory.tx(64, 10, Vec::new(), 1);
                if cost + tx.cost() > cost_limit / 4 {
                    break;
                }
                cost += tx.cost();
                txs.push(tx);
            }
        }
        let mut previous = None;
        let mut position = 0;
        loop {
            let chained = matches!(kind, "packages" | "mixed_packages")
                && position % 6 != 0
                && position % 6 < 3;
            let spent = if chained {
                previous.into_iter().collect()
            } else {
                Vec::new()
            };
            let tx = factory.tx(
                1,
                if chained && position % 6 == 2 { 4 } else { 1 },
                spent,
                1,
            );
            if cost + tx.cost() > cost_limit {
                break;
            }
            cost += tx.cost();
            previous = Some(OutPoint::from_usize(tx.transaction.id().mined_id(), 0));
            position += 1;
            txs.push(tx);
        }
        assert!(cost_limit - cost < transaction::MEMPOOL_TRANSACTION_COST_THRESHOLD);
        Self::from_transactions(txs)
    }
    fn snapshot(&self) -> (Vec<VerifiedUnminedTx>, TransactionDependencies) {
        (
            self.index
                .iter()
                .map(|key| self.pool[&key.id].clone())
                .collect(),
            self.deps.clone(),
        )
    }
}

#[derive(Clone, Copy)]
enum Mode {
    Random,
    Indexed,
    Packed,
}
impl Mode {
    fn name(self) -> &'static str {
        match self {
            Self::Random => "random",
            Self::Indexed => "indexed",
            Self::Packed => "indexed_pack",
        }
    }
}
struct Selection {
    txs: Vec<SelectedMempoolTx>,
    scanned: usize,
    packing_elapsed: Duration,
}
fn select(
    mode: Mode,
    txs: Vec<VerifiedUnminedTx>,
    deps: TransactionDependencies,
    network: &Network,
    height: Height,
    miner: &MinerParams,
) -> Selection {
    match mode {
        Mode::Random => Selection {
            txs: select_mempool_transactions(network, height, miner, None, txs, deps).unwrap(),
            scanned: 0,
            packing_elapsed: Duration::ZERO,
        },
        Mode::Indexed | Mode::Packed => {
            IndexedSelector::new(txs, deps, limits(network, height, miner))
                .finish(matches!(mode, Mode::Packed))
        }
    }
}

#[derive(Clone, Copy)]
struct Stats {
    selected: usize,
    bytes: usize,
    fees: u64,
    remaining: usize,
    scanned: usize,
    packing_ns: u64,
}
fn validate(
    selection: &Selection,
    fixture: &Fixture,
    network: &Network,
    height: Height,
    miner: &MinerParams,
) -> Stats {
    let mut limits = limits(network, height, miner);
    let mut selected = HashSet::new();
    let mut bytes = 0;
    let mut fees = 0;
    for (_, tx) in &selection.txs {
        let id = tx.transaction.id().mined_id();
        assert!(fixture.pool.contains_key(&id));
        assert!(parents(&fixture.deps, &id).all(|parent| selected.contains(&parent)));
        assert!(selected.insert(id));
        assert!(limits.try_add(tx));
        bytes += tx.transaction.size();
        let fee: u64 = tx.miner_fee.into();
        fees += fee;
    }
    Stats {
        selected: selected.len(),
        bytes,
        fees,
        remaining: limits.remaining_bytes,
        scanned: selection.scanned,
        packing_ns: selection.packing_elapsed.as_nanos().try_into().unwrap(),
    }
}

fn context() -> (Network, Height, MinerParams, GetBlockTemplateChainInfo) {
    let network = Network::Mainnet;
    let height = NetworkUpgrade::Nu6_3.activation_height(&network).unwrap();
    let miner = MinerParams::from(MinerAddress::from(TransparentAddress::PublicKeyHash(
        [0x7e; 20],
    )));
    let chain = GetBlockTemplateChainInfo {
        tip_hash: block::Hash([1; 32]),
        tip_height: height.previous().unwrap(),
        chain_history_root: Some(CHAIN_HISTORY_ACTIVATION_RESERVED.into()),
        expected_difficulty: CompactDifficulty::from(ExpandedDifficulty::from(U256::one())),
        cur_time: DateTime32::from(1_654_008_617),
        min_time: DateTime32::from(1_654_008_606),
        max_time: DateTime32::from(1_654_008_728),
        value_pools: ValueBalance::<NonNegative>::zero(),
    };
    (network, height, miner, chain)
}

#[test]
fn indexed_prototype_rescores_surviving_children_after_parent_selection() {
    let (network, height, miner, _) = context();
    let mut factory = Factory::new();
    let parent = factory.tx(1, 10, Vec::new(), 2);
    let parent_id = parent.transaction.id().mined_id();
    let child_a = factory.tx(1, 1, vec![OutPoint::from_usize(parent_id, 0)], 1);
    let child_b = factory.tx(1, 1, vec![OutPoint::from_usize(parent_id, 1)], 1);
    let independent = factory.tx(1, 2, Vec::new(), 1);
    let independent_id = independent.transaction.id().mined_id();
    let fixture = Fixture::from_transactions(vec![parent, child_a, child_b, independent]);
    let (txs, deps) = fixture.snapshot();
    let selection = select(Mode::Indexed, txs, deps, &network, height, &miner);
    validate(&selection, &fixture, &network, height, &miner);
    assert_eq!(selection.txs[0].1.transaction.id().mined_id(), parent_id);
    assert_eq!(
        selection.txs[1].1.transaction.id().mined_id(),
        independent_id
    );
}

#[test]
fn indexed_prototype_packing_preserves_priority_prefix_and_fills_the_gap() {
    let (network, height, miner, _) = context();
    let mut factory = Factory::new();
    let mut txs: Vec<_> = (0..12).map(|_| factory.tx(64, 10, Vec::new(), 1)).collect();
    txs.extend((0..40).map(|_| factory.tx(1, 1, Vec::new(), 1)));
    let fixture = Fixture::from_transactions(txs);
    let (txs, deps) = fixture.snapshot();
    let plain = select(Mode::Indexed, txs, deps, &network, height, &miner);
    let plain_stats = validate(&plain, &fixture, &network, height, &miner);
    assert!(plain_stats.remaining > PACKING_THRESHOLD_BYTES);
    let (txs, deps) = fixture.snapshot();
    let packed = select(Mode::Packed, txs, deps, &network, height, &miner);
    let packed_stats = validate(&packed, &fixture, &network, height, &miner);
    assert!(packed.txs.starts_with(&plain.txs));
    assert!(packed_stats.remaining < plain_stats.remaining);
    assert!(packed.scanned <= fixture.pool.len());
}

/// The env gate keeps all-ignored CI runs from executing this performance job.
#[test]
#[ignore = "manual full-mempool GBT performance measurement"]
#[allow(clippy::print_stdout, reason = "manual benchmark results")]
fn benchmark_full_mempool_gbt_selection() {
    let Ok(cost_limit) = std::env::var("ZAKURA_GBT_BENCH_COST_LIMIT") else {
        return;
    };
    let cost_limit: u64 = cost_limit.parse().unwrap();
    let (network, height, miner, chain) = context();
    for kind in ["uniform", "mixed", "packages", "mixed_packages"] {
        let fixture = Fixture::full(kind, cost_limit);
        for phase in ["selection", "template_json"] {
            let modes = [Mode::Random, Mode::Indexed, Mode::Packed];
            // Balance both position and predecessor, including warm-cache
            // effects when one indexed variant immediately follows the other.
            let orders = [
                [0, 1, 2],
                [2, 1, 0],
                [1, 2, 0],
                [0, 2, 1],
                [2, 0, 1],
                [1, 0, 2],
            ];
            let mut results: [Vec<(Duration, Stats)>; 3] = Default::default();
            for sample in 0..=SAMPLES {
                for index in orders[sample % orders.len()] {
                    let mode = modes[index];
                    let mut elapsed = Duration::ZERO;
                    let selection = if phase == "selection" {
                        let (txs, deps) = fixture.snapshot();
                        let start = Instant::now();
                        let result = black_box(select(mode, txs, deps, &network, height, &miner));
                        elapsed += start.elapsed();
                        result
                    } else {
                        let start = Instant::now();
                        let (txs, deps) = fixture.snapshot();
                        let result = black_box(select(mode, txs, deps, &network, height, &miner));
                        elapsed += start.elapsed();
                        result
                    };
                    let stats = validate(&selection, &fixture, &network, height, &miner);
                    if phase == "template_json" {
                        let start = Instant::now();
                        let template = BlockTemplateResponse::new_internal(
                            &network,
                            None,
                            &miner,
                            &chain,
                            "0".repeat(46).parse().unwrap(),
                            selection.txs,
                            None,
                        )
                        .unwrap();
                        black_box(serde_json::to_vec(&template).unwrap());
                        elapsed += start.elapsed();
                        let block =
                            proposal_block_from_template(&template, None, &network).unwrap();
                        assert!(
                            u64::try_from(block.zcash_serialized_size()).unwrap()
                                <= MAX_BLOCK_BYTES
                        );
                    }
                    if sample > 0 {
                        results[index].push((elapsed, stats));
                    }
                }
            }
            for (mode, mut result) in modes.into_iter().zip(results) {
                result.sort_by_key(|(elapsed, _)| *elapsed);
                let median =
                    (result[SAMPLES / 2 - 1].0.as_nanos() + result[SAMPLES / 2].0.as_nanos()) / 2;
                let mut packing_times: Vec<_> =
                    result.iter().map(|(_, stats)| stats.packing_ns).collect();
                packing_times.sort();
                let packing_ns = (packing_times[SAMPLES / 2 - 1] + packing_times[SAMPLES / 2]) / 2;
                let values = |extract: fn(Stats) -> u64| -> (u64, u64) {
                    let mut values: Vec<_> =
                        result.iter().map(|(_, stats)| extract(*stats)).collect();
                    values.sort();
                    (values[0], values[values.len() - 1])
                };
                let (selected_min, selected_max) = values(|s| u64::try_from(s.selected).unwrap());
                let (bytes_min, bytes_max) = values(|s| u64::try_from(s.bytes).unwrap());
                let (fees_min, fees_max) = values(|s| s.fees);
                let (remaining_min, remaining_max) =
                    values(|s| u64::try_from(s.remaining).unwrap());
                let (scanned_min, scanned_max) = values(|s| u64::try_from(s.scanned).unwrap());
                println!("gbt fixture={kind} cost_limit={cost_limit} pool_cost={} pool_txs={} phase={phase} mode={} median_ns={median} min_ns={} max_ns={} selected_min={selected_min} selected_max={selected_max} bytes_min={bytes_min} bytes_max={bytes_max} fees_min={fees_min} fees_max={fees_max} remaining_min={remaining_min} remaining_max={remaining_max} scanned_min={scanned_min} scanned_max={scanned_max} packing_ns={packing_ns}",
                    fixture.cost, fixture.pool.len(), mode.name(), result[0].0.as_nanos(), result[SAMPLES-1].0.as_nanos());
            }
        }
    }
}
