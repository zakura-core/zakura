//! Indexed selection tests and the GBT CPU benchmark.
//!
//! Fixtures have canonical encoded shapes and unique nullifiers, but
//! placeholder proofs/signatures are not verified.

use std::{
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

use super::{
    indexed::{
        parents, priority, IndexedSelector, Pool, Priority, MAX_PACKING_SCAN_ENTRIES,
        PACKING_THRESHOLD_BYTES, SELECTION_STOP_BYTES,
    },
    *,
};
use crate::methods::types::get_block_template::{
    proposal::proposal_block_from_template, BlockTemplateResponse, MinerAddress,
};

const SAMPLES: usize = 12;

fn limits(network: &Network, height: Height, miner: &MinerParams) -> BlockTemplateLimits {
    let coinbase =
        TransactionTemplate::coinbase_resource_usage(network, height, miner, None).unwrap();
    BlockTemplateLimits::initial(network, height, coinbase)
}

/// Selected transactions, the number of entries packing visited, and the
/// time packing took.
struct Selection {
    txs: Vec<SelectedMempoolTx>,
    packing_scanned: usize,
    packing_elapsed: Duration,
}

/// Runs the primary passes, then packing if `pack` is set.
fn run(
    txs: Vec<VerifiedUnminedTx>,
    deps: TransactionDependencies,
    limits: BlockTemplateLimits,
    pack: bool,
) -> Selection {
    let mut selector = IndexedSelector::new(txs, deps, limits);
    selector.select_primary();
    let (packing_scanned, packing_elapsed) = if pack {
        let start = Instant::now();
        let scanned = selector.pack();
        (scanned, start.elapsed())
    } else {
        (0, Duration::ZERO)
    };
    Selection {
        txs: selector.selected().to_vec(),
        packing_scanned,
        packing_elapsed,
    }
}

fn ids(selection: &Selection) -> Vec<transaction::Hash> {
    selection
        .txs
        .iter()
        .map(|(_, tx)| tx.transaction.id().mined_id())
        .collect()
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
    index: BTreeSet<Priority>,
    deps: TransactionDependencies,
    cost: u64,
}
impl Fixture {
    fn from_transactions(txs: Vec<VerifiedUnminedTx>) -> Self {
        let mut deps = TransactionDependencies::default();
        let mut pool = Pool::new();
        let mut cost = 0;
        for tx in txs {
            let id = tx.transaction.id().mined_id();
            deps.add(id, tx.transaction.transaction().spent_outpoints().collect());
            cost += tx.cost();
            pool.insert(id, tx);
        }
        let selected = std::collections::HashMap::new();
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

/// Selects with the production entry point's limits, with or without packing.
fn select(
    pack: bool,
    txs: Vec<VerifiedUnminedTx>,
    deps: TransactionDependencies,
    network: &Network,
    height: Height,
    miner: &MinerParams,
) -> Selection {
    let limits = limits(network, height, miner);
    run(txs, deps, limits, pack)
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
    assert!(selection.packing_scanned <= MAX_PACKING_SCAN_ENTRIES);
    let mut limits = limits(network, height, miner);
    let mut selected = std::collections::HashSet::new();
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
        scanned: selection.packing_scanned,
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
fn indexed_rescores_surviving_children_after_parent_selection() {
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
    let selection = select(false, txs, deps, &network, height, &miner);
    validate(&selection, &fixture, &network, height, &miner);
    assert_eq!(selection.txs[0].1.transaction.id().mined_id(), parent_id);
    assert_eq!(
        selection.txs[1].1.transaction.id().mined_id(),
        independent_id
    );
}

#[test]
fn indexed_packing_preserves_priority_prefix_and_fills_the_gap() {
    let (network, height, miner, _) = context();
    let mut factory = Factory::new();
    let mut txs: Vec<_> = (0..12).map(|_| factory.tx(64, 10, Vec::new(), 1)).collect();
    txs.extend((0..40).map(|_| factory.tx(1, 1, Vec::new(), 1)));
    let fixture = Fixture::from_transactions(txs);
    let (txs, deps) = fixture.snapshot();
    let plain = select(false, txs, deps, &network, height, &miner);
    let plain_stats = validate(&plain, &fixture, &network, height, &miner);
    assert!(plain_stats.remaining > PACKING_THRESHOLD_BYTES);
    let (txs, deps) = fixture.snapshot();
    let packed = select(true, txs, deps, &network, height, &miner);
    let packed_stats = validate(&packed, &fixture, &network, height, &miner);
    assert!(packed.txs.starts_with(&plain.txs));
    assert!(packed_stats.remaining < plain_stats.remaining);
    assert!(packed.packing_scanned <= MAX_PACKING_SCAN_ENTRIES);
}

#[test]
fn indexed_packing_stops_after_one_hundred_entries() {
    let (network, height, miner, _) = context();
    let mut factory = Factory::new();
    let mut txs: Vec<_> = (0..=MAX_PACKING_SCAN_ENTRIES)
        .map(|_| factory.tx(64, 10, Vec::new(), 1))
        .collect();
    let small = factory.tx(1, 1, Vec::new(), 1);
    let small_id = small.transaction.id().mined_id();
    txs.push(small);
    let fixture = Fixture::from_transactions(txs);
    let run = |mut txs: Vec<VerifiedUnminedTx>, deps, remove_first| {
        if remove_first {
            txs.remove(0);
        }
        let mut limits = limits(&network, height, &miner);
        limits.remaining_bytes = PACKING_THRESHOLD_BYTES + 1;
        run(txs, deps, limits, true)
    };

    // The small transaction follows 100 oversized entries after the blocked
    // priority candidate. It must not be visited as entry 101.
    let (txs, deps) = fixture.snapshot();
    let bounded = run(txs, deps, false);
    assert_eq!(bounded.packing_scanned, MAX_PACKING_SCAN_ENTRIES);
    assert!(bounded.txs.is_empty());

    // Removing one oversized transaction puts the fitting candidate exactly at
    // the scan boundary, proving entry 100 is still considered.
    let (txs, deps) = fixture.snapshot();
    let boundary = run(txs, deps, true);
    validate(&boundary, &fixture, &network, height, &miner);
    assert_eq!(boundary.packing_scanned, MAX_PACKING_SCAN_ENTRIES);
    assert_eq!(boundary.txs.len(), 1);
    assert_eq!(boundary.txs[0].1.transaction.id().mined_id(), small_id);
}

#[test]
fn indexed_primary_stops_at_three_kilobytes() {
    let (network, height, miner, _) = context();
    let mut factory = Factory::new();
    let first = factory.tx(0, 10, Vec::new(), 1);
    let first_size = first.transaction.size();
    let first_id = first.transaction.id().mined_id();
    let tail = factory.tx(0, 1, Vec::new(), 2);
    assert!(tail.transaction.size() < SELECTION_STOP_BYTES);
    let fixture = Fixture::from_transactions(vec![first, tail]);
    for pack in [false, true] {
        for remaining in [SELECTION_STOP_BYTES - 1, SELECTION_STOP_BYTES] {
            let (txs, deps) = fixture.snapshot();
            let mut limits = limits(&network, height, &miner);
            limits.remaining_bytes = remaining;
            let selection = run(txs, deps, limits, pack);
            assert!(selection.txs.is_empty());
            assert_eq!(selection.packing_scanned, 0);
        }
        let (txs, deps) = fixture.snapshot();
        let mut limits = limits(&network, height, &miner);
        limits.remaining_bytes = first_size + SELECTION_STOP_BYTES;
        let selection = run(txs, deps, limits, pack);
        validate(&selection, &fixture, &network, height, &miner);
        assert_eq!(selection.txs.len(), 1);
        assert_eq!(selection.txs[0].1.transaction.id().mined_id(), first_id);
    }
}

#[test]
fn indexed_packing_stops_at_three_kilobytes() {
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
    let selection = run(txs, deps, limits, true);
    let stats = validate(&selection, &fixture, &network, height, &miner);
    assert_eq!(remaining - stats.bytes, SELECTION_STOP_BYTES);
    assert_eq!(selection.packing_scanned, 8);
    assert_eq!(selection.txs.len(), 8);
    assert!(selection
        .txs
        .iter()
        .all(|(_, tx)| tx.transaction.id().mined_id() != tail_id));
}

#[test]
fn indexed_selection_is_repeatable_and_orders_package_parents_by_txid() {
    let (network, height, miner, _) = context();
    let mut factory = Factory::new();
    let parent_a = factory.tx(1, 1, Vec::new(), 1);
    let parent_b = factory.tx(1, 1, Vec::new(), 1);
    let parent_ids = [
        parent_a.transaction.id().mined_id(),
        parent_b.transaction.id().mined_id(),
    ];
    let child = factory.tx(
        1,
        100,
        parent_ids
            .iter()
            .map(|id| OutPoint::from_usize(*id, 0))
            .collect(),
        1,
    );
    let child_id = child.transaction.id().mined_id();
    let independent = factory.tx(1, 2, Vec::new(), 1);
    let fixture = Fixture::from_transactions(vec![parent_a, parent_b, child, independent]);

    let runs: Vec<_> = (0..8)
        .map(|_| {
            let (txs, deps) = fixture.snapshot();
            let selection = select(true, txs, deps, &network, height, &miner);
            validate(&selection, &fixture, &network, height, &miner);
            ids(&selection)
        })
        .collect();
    assert!(runs.windows(2).all(|pair| pair[0] == pair[1]));

    // The child's package ranks first, so both parents precede it in TXID order.
    let (low, high) = (
        parent_ids[0].min(parent_ids[1]),
        parent_ids[0].max(parent_ids[1]),
    );
    assert_eq!(runs[0][..3], [low, high, child_id]);
}

#[test]
fn indexed_skips_candidates_with_unavailable_ancestors() {
    let (network, height, miner, _) = context();
    let mut factory = Factory::new();
    let orphan = factory.tx(
        1,
        100,
        vec![OutPoint::from_usize(transaction::Hash([0xab; 32]), 0)],
        1,
    );
    let orphan_id = orphan.transaction.id().mined_id();
    let available = factory.tx(1, 1, Vec::new(), 1);
    let available_id = available.transaction.id().mined_id();

    let mut deps = TransactionDependencies::default();
    deps.add(
        orphan_id,
        orphan.transaction.transaction().spent_outpoints().collect(),
    );
    deps.add(available_id, Vec::new());

    // The orphan comes first, as its baseline score would rank it.
    let selection = select(
        true,
        vec![orphan, available],
        deps,
        &network,
        height,
        &miner,
    );
    assert_eq!(ids(&selection), [available_id]);
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
            // (name, pack): the main pass alone, then with packing, so the
            // difference shows what packing costs.
            let modes = [("indexed", false), ("indexed_pack", true)];
            // Alternate which mode runs first, balancing warm-cache effects
            // when one mode immediately follows the other.
            let orders = [[0, 1], [1, 0]];
            let mut results: [Vec<(Duration, Stats)>; 2] = Default::default();
            // Sample 0 warms caches and is discarded.
            for sample in 0..=SAMPLES {
                for index in orders[sample % orders.len()] {
                    let (_, pack) = modes[index];
                    let mut elapsed = Duration::ZERO;
                    let selection = if phase == "selection" {
                        let (txs, deps) = fixture.snapshot();
                        let start = Instant::now();
                        let result = black_box(select(pack, txs, deps, &network, height, &miner));
                        elapsed += start.elapsed();
                        result
                    } else {
                        let start = Instant::now();
                        let (txs, deps) = fixture.snapshot();
                        let result = black_box(select(pack, txs, deps, &network, height, &miner));
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

            for ((mode, _), mut result) in modes.into_iter().zip(results) {
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
                println!("gbt fixture={kind} cost_limit={cost_limit} pool_cost={} pool_txs={} phase={phase} mode={mode} median_ns={median} min_ns={} max_ns={} selected_min={selected_min} selected_max={selected_max} bytes_min={bytes_min} bytes_max={bytes_max} fees_min={fees_min} fees_max={fees_max} remaining_min={remaining_min} remaining_max={remaining_max} scanned_min={scanned_min} scanned_max={scanned_max} packing_ns={packing_ns}",
                    fixture.cost, fixture.pool.len(), result[0].0.as_nanos(), result[SAMPLES-1].0.as_nanos());
            }
        }
    }
}
