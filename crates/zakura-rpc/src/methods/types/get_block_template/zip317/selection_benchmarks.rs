//! Mining-order selection tests and full-mempool GBT CPU benchmarks.
//!
//! Fixtures have canonical encoded shapes and unique nullifiers, but
//! placeholder proofs/signatures are not verified.

use std::{
    collections::{BTreeSet, HashMap, HashSet},
    hint::black_box,
    sync::Arc,
    time::{Duration, Instant},
};

use proptest::{prelude::*, strategy::ValueTree, test_runner::TestRunner};
use rand_10::{
    distr::{Distribution, Open01},
    rng as thread_rng, Rng,
};
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

use super::{
    mining_order::{
        package_priority, PackagePriority, Pool, MAX_PACKING_SCAN_ENTRIES, PACKING_THRESHOLD_BYTES,
        SELECTION_STOP_BYTES,
    },
    *,
};
use crate::methods::types::get_block_template::{
    proposal::proposal_block_from_template, BlockTemplateResponse, MinerAddress,
};

const SAMPLES: usize = 12;

fn parents<'a>(
    deps: &'a TransactionDependencies,
    id: &transaction::Hash,
) -> impl Iterator<Item = transaction::Hash> + 'a {
    deps.dependencies().get(id).into_iter().flatten().copied()
}

fn limits(network: &Network, height: Height, miner: &MinerParams) -> BlockTemplateLimits {
    let coinbase =
        TransactionTemplate::coinbase_resource_usage(network, height, miner, None).unwrap();
    BlockTemplateLimits::initial(network, height, coinbase)
}

/// The fee-weighted random selection that templates used before the mining
/// order, kept as the benchmark baseline.
///
/// It scores, shuffles, and sorts every mempool transaction for each template.
fn select_fee_weighted_random(
    mempool_txs: Vec<VerifiedUnminedTx>,
    mempool_tx_deps: TransactionDependencies,
    mut limits: BlockTemplateLimits,
) -> Vec<SelectedMempoolTx> {
    let tx_dependencies = mempool_tx_deps.dependencies();
    let (independent_mempool_txs, mut dependent_mempool_txs): (HashMap<_, _>, HashMap<_, _>) =
        mempool_txs
            .into_iter()
            .map(|tx| (tx.transaction.id().mined_id(), tx))
            .partition(|(tx_id, _tx)| !tx_dependencies.contains_key(tx_id));

    let (conventional_fee_txs, low_fee_txs): (Vec<_>, Vec<_>) = independent_mempool_txs
        .into_values()
        .partition(VerifiedUnminedTx::pays_conventional_fee);

    let mut selected_txs = Vec::new();
    let mut rng = thread_rng();

    // Exhaust conventional-fee candidates before considering low-fee ones.
    for candidates in [conventional_fee_txs, low_fee_txs] {
        for candidate_tx in fee_weighted_order(candidates, &mut rng) {
            checked_add_transaction(
                candidate_tx,
                &mut dependent_mempool_txs,
                &mut selected_txs,
                &mempool_tx_deps,
                &mut limits,
            );
        }
    }

    selected_txs
}

/// Orders candidates by fee weight without replacement in O(n log n) time.
///
/// Independent exponential clocks with rates equal to the fee weights give
/// each remaining candidate a probability proportional to its weight of being
/// next. Sorting those clocks is equivalent to repeated weighted draws, without
/// rebuilding cumulative weights after every draw or accumulating subtraction
/// error. Each candidate occurs exactly once, even if it does not fit the block.
fn fee_weighted_order(
    transactions: Vec<VerifiedUnminedTx>,
    rng: &mut impl Rng,
) -> impl Iterator<Item = VerifiedUnminedTx> {
    let mut candidates: Vec<_> = transactions
        .into_iter()
        .map(|tx| {
            // Verified transactions have finite, strictly positive fee weights.
            // Open01 keeps the logarithm finite and strictly negative.
            let uniform: f64 = Open01.sample(rng);
            let clock = -uniform.ln() / f64::from(tx.fee_weight_ratio);
            (clock, tx)
        })
        .collect();

    candidates.sort_unstable_by(|(left, _), (right, _)| left.total_cmp(right));
    candidates.into_iter().map(|(_, tx)| tx)
}

/// Checks if every item in `candidate_tx_deps` is present in `selected_txs`.
///
/// Requires items in `selected_txs` to be unique to work correctly.
fn has_direct_dependencies(
    candidate_tx_deps: Option<&HashSet<transaction::Hash>>,
    selected_txs: &Vec<SelectedMempoolTx>,
) -> bool {
    let Some(deps) = candidate_tx_deps else {
        return true;
    };

    if selected_txs.len() < deps.len() {
        return false;
    }

    let mut num_available_deps = 0;
    for tx in selected_txs {
        let (_, tx) = tx;
        if deps.contains(&tx.transaction.id().mined_id()) {
            num_available_deps += 1;
        } else {
            continue;
        }

        if num_available_deps == deps.len() {
            return true;
        }
    }

    false
}

/// Tries to add `candidate_tx` and its dependents to `selected_txs`.
///
/// If it fits in the supplied limits, adds it to `selected_txs`, updates the
/// limits, and considers dependents whose dependencies have all been selected.
fn checked_add_transaction(
    candidate_tx: VerifiedUnminedTx,
    dependent_txs: &mut HashMap<transaction::Hash, VerifiedUnminedTx>,
    selected_txs: &mut Vec<SelectedMempoolTx>,
    mempool_tx_deps: &TransactionDependencies,
    limits: &mut BlockTemplateLimits,
) {
    if !limits.try_add(&candidate_tx) {
        return;
    }

    let tx_dependencies = mempool_tx_deps.dependencies();
    let selected_tx_id = &candidate_tx.transaction.id().mined_id();
    debug_assert!(
        !tx_dependencies.contains_key(selected_tx_id),
        "all candidate transactions should be independent"
    );

    selected_txs.push((0, candidate_tx));

    // Try adding any dependent transactions if all of their dependencies have been selected.

    let mut current_level_dependents = mempool_tx_deps.direct_dependents(selected_tx_id);
    while !current_level_dependents.is_empty() {
        let mut next_level_dependents = HashSet::new();

        for dependent_tx_id in &current_level_dependents {
            // ## Note
            //
            // A necessary condition for adding the dependent tx is that it spends unmined outputs coming only from
            // the selected txs, which come from the mempool. If the tx also spends in-chain outputs, it won't
            // be added. This behavior is not specified by consensus rules and can be changed at any time,
            // meaning that such txs could be added.
            if has_direct_dependencies(tx_dependencies.get(dependent_tx_id), selected_txs) {
                let Some(candidate_tx) = dependent_txs.remove(dependent_tx_id) else {
                    continue;
                };

                // Transactions that don't pay the conventional fee should not have
                // the same probability of being included as their dependencies.
                if !candidate_tx.pays_conventional_fee() {
                    continue;
                }

                if !limits.try_add(&candidate_tx) {
                    continue;
                }

                selected_txs.push((
                    dependencies_depth(dependent_tx_id, mempool_tx_deps),
                    candidate_tx,
                ));

                next_level_dependents.extend(mempool_tx_deps.direct_dependents(dependent_tx_id));
            }
        }

        current_level_dependents = next_level_dependents;
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
        if actions == 0 {
            self.serial += 1;
        }
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
            ironwood_shielded_data: (actions > 0).then(|| ironwood::ShieldedData {
                flags: ironwood::Flags::ENABLE_SPENDS,
                value_balance: Amount::zero(),
                shared_anchor: Default::default(),
                proof: Halo2Proof(vec![0; ::orchard::Proof::expected_proof_size(actions)]),
                actions: authorized.try_into().unwrap(),
                binding_sig: [0; 64].into(),
            }),
        };
        if let Some(shielded) = tx.ironwood_shielded_data() {
            assert!(shielded.proof_size_is_canonical());
        }
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
    index: BTreeSet<PackagePriority>,
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
        let selected = HashSet::new();
        let index = pool
            .keys()
            .map(|id| package_priority(*id, &pool, &selected, &deps).unwrap())
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
    FeeWeightedRandom,
    MiningOrder,
}
impl Mode {
    fn name(self) -> &'static str {
        match self {
            Self::FeeWeightedRandom => "fee_weighted_random",
            Self::MiningOrder => "mining_order",
        }
    }
}

fn select(
    mode: Mode,
    txs: Vec<VerifiedUnminedTx>,
    deps: TransactionDependencies,
    limits: BlockTemplateLimits,
) -> Vec<SelectedMempoolTx> {
    match mode {
        Mode::FeeWeightedRandom => select_fee_weighted_random(txs, deps, limits),
        Mode::MiningOrder => MiningOrderSelector::new(txs, deps, limits).select(),
    }
}

fn ids(selection: &[SelectedMempoolTx]) -> Vec<transaction::Hash> {
    selection
        .iter()
        .map(|(_, tx)| tx.transaction.id().mined_id())
        .collect()
}

#[derive(Clone, Copy)]
struct Stats {
    selected: usize,
    bytes: usize,
    fees: u64,
    remaining: usize,
}

/// Checks that `selection` is a valid template for `limits` drawn from
/// `fixture`, with parents before children, and returns its totals.
fn validate(
    selection: &[SelectedMempoolTx],
    fixture: &Fixture,
    mut limits: BlockTemplateLimits,
) -> Stats {
    let mut selected = HashSet::new();
    let mut bytes = 0;
    let mut fees = 0;
    for (_, tx) in selection {
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
fn selection_rescores_surviving_children_after_parent_selection() {
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
    let selection = select(
        Mode::MiningOrder,
        txs,
        deps,
        limits(&network, height, &miner),
    );
    let stats = validate(&selection, &fixture, limits(&network, height, &miner));
    assert_eq!(stats.selected, 4);
    // Once the parent is selected, each child is scored on its own fee, which
    // is lower than the independent transaction's.
    assert_eq!(ids(&selection)[..2], [parent_id, independent_id]);
}

#[test]
fn selection_pulls_in_a_low_fee_parent_for_a_high_fee_child() {
    let (network, height, miner, _) = context();
    let mut factory = Factory::new();
    let parent = factory.tx(1, 1, Vec::new(), 1);
    let parent_id = parent.transaction.id().mined_id();
    let child = factory.tx(1, 9, vec![OutPoint::from_usize(parent_id, 0)], 1);
    let child_id = child.transaction.id().mined_id();
    let independent = factory.tx(1, 3, Vec::new(), 1);
    let independent_id = independent.transaction.id().mined_id();
    let fixture = Fixture::from_transactions(vec![parent, child, independent]);
    let (txs, deps) = fixture.snapshot();
    let selection = select(
        Mode::MiningOrder,
        txs,
        deps,
        limits(&network, height, &miner),
    );
    validate(&selection, &fixture, limits(&network, height, &miner));
    // The package pays five times the conventional fee per transaction, which
    // beats the independent transaction. The parent alone would come last.
    assert_eq!(ids(&selection), [parent_id, child_id, independent_id]);
}

#[test]
fn selection_is_deterministic() {
    let (network, height, miner, _) = context();
    let fixture = Fixture::full("mixed_packages", 4_000_000);
    let select = || {
        let (txs, deps) = fixture.snapshot();
        select(
            Mode::MiningOrder,
            txs,
            deps,
            limits(&network, height, &miner),
        )
    };
    let first = select();
    assert!(!first.is_empty());
    assert_eq!(ids(&first), ids(&select()));
}

#[test]
fn packing_keeps_the_priority_prefix_and_fills_the_gap() {
    let (network, height, miner, _) = context();
    let mut factory = Factory::new();
    let large: Vec<_> = (0..12).map(|_| factory.tx(64, 10, Vec::new(), 1)).collect();
    let large_size = large[0].transaction.size();
    let large_ids: Vec<_> = large
        .iter()
        .map(|tx| tx.transaction.id().mined_id())
        .collect();
    let mut txs = large;
    txs.extend((0..40).map(|_| factory.tx(1, 1, Vec::new(), 1)));
    let fixture = Fixture::from_transactions(txs);
    let (txs, deps) = fixture.snapshot();
    let selection = select(
        Mode::MiningOrder,
        txs,
        deps,
        limits(&network, height, &miner),
    );
    let stats = validate(&selection, &fixture, limits(&network, height, &miner));

    // The priority pass stops at the first large transaction that does not fit.
    let prefix = limits(&network, height, &miner).remaining_bytes / large_size;
    assert!(prefix < large_ids.len());
    assert_eq!(ids(&selection)[..prefix], large_ids[..prefix]);
    // Packing then adds small transactions from later in the order.
    assert!(selection.len() > prefix);
    assert!(stats.remaining < large_size);
}

#[test]
fn packing_adds_a_package_with_its_unselected_parent() {
    let (network, height, miner, _) = context();
    let mut factory = Factory::new();
    let large = factory.tx(64, 10, Vec::new(), 1);
    let parent = factory.tx(1, 1, Vec::new(), 1);
    let parent_id = parent.transaction.id().mined_id();
    let child = factory.tx(1, 5, vec![OutPoint::from_usize(parent_id, 0)], 1);
    let child_id = child.transaction.id().mined_id();
    let package_size = parent.transaction.size() + child.transaction.size();
    assert!(large.transaction.size() > PACKING_THRESHOLD_BYTES + package_size);
    let fixture = Fixture::from_transactions(vec![large, parent, child]);
    let (txs, deps) = fixture.snapshot();
    let mut limits = limits(&network, height, &miner);
    limits.remaining_bytes = PACKING_THRESHOLD_BYTES + package_size;

    // The large transaction blocks the priority pass. The child is next in
    // the order, ahead of its lower-fee parent.
    let selection = MiningOrderSelector::new(txs, deps, limits.clone()).select();
    validate(&selection, &fixture, limits);
    assert_eq!(ids(&selection), [parent_id, child_id]);
}

#[test]
fn packing_stops_after_one_hundred_entries() {
    let (network, height, miner, _) = context();
    let mut factory = Factory::new();
    let mut txs: Vec<_> = (0..=MAX_PACKING_SCAN_ENTRIES)
        .map(|_| factory.tx(64, 10, Vec::new(), 1))
        .collect();
    let small = factory.tx(1, 1, Vec::new(), 1);
    let small_id = small.transaction.id().mined_id();
    txs.push(small);
    let fixture = Fixture::from_transactions(txs);
    let run = |remove_first| {
        let (mut txs, deps) = fixture.snapshot();
        if remove_first {
            txs.remove(0);
        }
        let mut limits = limits(&network, height, &miner);
        limits.remaining_bytes = PACKING_THRESHOLD_BYTES + 1;
        MiningOrderSelector::new(txs, deps, limits).select()
    };

    // The small transaction follows 100 oversized entries after the blocked
    // priority candidate. It must not be visited as entry 101.
    assert!(run(false).is_empty());

    // Removing one oversized transaction puts the fitting candidate exactly at
    // the scan boundary, proving entry 100 is still considered.
    assert_eq!(ids(&run(true)), [small_id]);
}

#[test]
fn priority_pass_stops_at_three_kilobytes() {
    let (network, height, miner, _) = context();
    let mut factory = Factory::new();
    let first = factory.tx(0, 10, Vec::new(), 1);
    let first_size = first.transaction.size();
    let first_id = first.transaction.id().mined_id();
    let tail = factory.tx(0, 1, Vec::new(), 2);
    assert!(tail.transaction.size() < SELECTION_STOP_BYTES);
    let fixture = Fixture::from_transactions(vec![first, tail]);
    let run = |remaining| {
        let (txs, deps) = fixture.snapshot();
        let mut limits = limits(&network, height, &miner);
        limits.remaining_bytes = remaining;
        MiningOrderSelector::new(txs, deps, limits).select()
    };

    assert!(run(SELECTION_STOP_BYTES - 1).is_empty());
    assert!(run(SELECTION_STOP_BYTES).is_empty());
    assert_eq!(ids(&run(first_size + SELECTION_STOP_BYTES)), [first_id]);
}

#[test]
fn packing_stops_at_three_kilobytes() {
    let (network, height, miner, _) = context();
    let mut factory = Factory::new();
    let mut txs = vec![factory.tx(64, 10, Vec::new(), 1)];
    txs.extend((0..8).map(|_| factory.tx(1, 2, Vec::new(), 1)));
    let remaining = txs[1..]
        .iter()
        .map(|tx| tx.transaction.size())
        .sum::<usize>()
        + SELECTION_STOP_BYTES;
    assert!(remaining > PACKING_THRESHOLD_BYTES);
    let tail = factory.tx(0, 1, Vec::new(), 1);
    let tail_id = tail.transaction.id().mined_id();
    assert!(tail.transaction.size() < SELECTION_STOP_BYTES);
    txs.push(tail);
    let fixture = Fixture::from_transactions(txs);
    let (txs, deps) = fixture.snapshot();
    let mut limits = limits(&network, height, &miner);
    limits.remaining_bytes = remaining;
    let selection = MiningOrderSelector::new(txs, deps, limits.clone()).select();
    let stats = validate(&selection, &fixture, limits);
    assert_eq!(stats.remaining, SELECTION_STOP_BYTES);
    assert_eq!(selection.len(), 8);
    assert!(!ids(&selection).contains(&tail_id));
}

/// Compares template selection from the mining order with the fee-weighted
/// random selection it replaced, on mempools filled to a cost limit.
///
/// The `selection` phase times selection alone. The `template_json` phase also
/// times copying the mempool snapshot, building the template, and encoding it.
///
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
            let modes = [Mode::FeeWeightedRandom, Mode::MiningOrder];
            let mut results: [Vec<(Duration, Stats)>; 2] = Default::default();
            // The first sample warms caches. Later samples alternate which
            // mode runs first.
            for sample in 0..=SAMPLES {
                for index in [sample % 2, (sample + 1) % 2] {
                    let mode = modes[index];
                    let template_limits = limits(&network, height, &miner);
                    let mut elapsed = Duration::ZERO;
                    let selection = if phase == "selection" {
                        let (txs, deps) = fixture.snapshot();
                        let start = Instant::now();
                        let result = black_box(select(mode, txs, deps, template_limits));
                        elapsed += start.elapsed();
                        result
                    } else {
                        let start = Instant::now();
                        let (txs, deps) = fixture.snapshot();
                        let result = black_box(select(mode, txs, deps, template_limits));
                        elapsed += start.elapsed();
                        result
                    };
                    let stats = validate(&selection, &fixture, limits(&network, height, &miner));
                    if phase == "template_json" {
                        let start = Instant::now();
                        let template = BlockTemplateResponse::new_internal(
                            &network,
                            None,
                            &miner,
                            &chain,
                            "0".repeat(46).parse().unwrap(),
                            selection,
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
                println!(
                    "gbt fixture={kind} cost_limit={cost_limit} pool_cost={} pool_txs={} \
                     phase={phase} mode={} median_ns={median} min_ns={} max_ns={} \
                     selected_min={selected_min} selected_max={selected_max} \
                     bytes_min={bytes_min} bytes_max={bytes_max} \
                     fees_min={fees_min} fees_max={fees_max} \
                     remaining_min={remaining_min} remaining_max={remaining_max}",
                    fixture.cost,
                    fixture.pool.len(),
                    mode.name(),
                    result[0].0.as_nanos(),
                    result[SAMPLES - 1].0.as_nanos(),
                );
            }
        }
    }
}
