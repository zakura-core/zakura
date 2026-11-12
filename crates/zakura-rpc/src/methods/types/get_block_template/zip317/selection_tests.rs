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
use rand_10::{rngs::StdRng, SeedableRng};
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
        parents, priority, IndexedSelector, Pool, Priority, MAX_CONSECUTIVE_MISFITS,
        SELECTION_STOP_BYTES,
    },
    selection_variants::{weighted_random, Misfit, Score, VariantSelector},
    *,
};
use crate::methods::types::get_block_template::{
    proposal::proposal_block_from_template, BlockTemplateResponse, MinerAddress,
};

const SAMPLES: usize = 12;

/// Signature (72 bytes) and compressed public key (33 bytes) pushes.
const P2PKH_UNLOCK_SCRIPT_BYTES: usize = 107;

/// Chain inputs and outputs of the rotating transparent transaction shapes:
/// payments, a consolidation, and a payout.
const TRANSPARENT_SHAPES: [(usize, usize); 6] = [(1, 2), (2, 2), (1, 1), (5, 1), (1, 20), (3, 2)];

/// Rotating fee multiples of the conventional fee for transparent fixtures.
const TRANSPARENT_FEE_MULTIPLES: [u64; 5] = [1, 1, 2, 1, 3];

fn limits(network: &Network, height: Height, miner: &MinerParams) -> BlockTemplateLimits {
    let coinbase =
        TransactionTemplate::coinbase_resource_usage(network, height, miner, None).unwrap();
    BlockTemplateLimits::initial(network, height, coinbase)
}

/// Selected transactions, and how many best packages did not fit when the
/// selector reports it.
struct Selection {
    txs: Vec<SelectedMempoolTx>,
    misfits: usize,
}

/// Runs the production selector.
fn run(
    txs: Vec<VerifiedUnminedTx>,
    deps: TransactionDependencies,
    limits: BlockTemplateLimits,
) -> Selection {
    Selection {
        txs: IndexedSelector::new(txs, deps, limits).select(),
        misfits: 0,
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
        self.build(actions, fee_multiple, spent, 0, outputs)
    }

    /// A transparent transaction spending `chain_inputs` fresh chain outputs
    /// and the mempool outputs in `spent`, each with a P2PKH-sized unlock
    /// script, paying to `outputs` P2PKH outputs.
    fn transparent(
        &mut self,
        chain_inputs: usize,
        spent: Vec<OutPoint>,
        outputs: usize,
        fee_multiple: u64,
    ) -> VerifiedUnminedTx {
        let mut inputs = spent;
        for _ in 0..chain_inputs {
            self.serial += 1;
            let mut hash = [0xcc; 32];
            hash[..8].copy_from_slice(&self.serial.to_le_bytes());
            inputs.push(OutPoint::from_usize(transaction::Hash(hash), 0));
        }
        self.build(0, fee_multiple, inputs, P2PKH_UNLOCK_SCRIPT_BYTES, outputs)
    }

    fn build(
        &mut self,
        actions: usize,
        fee_multiple: u64,
        spent: Vec<OutPoint>,
        unlock_script_bytes: usize,
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
                    unlock_script: Script::new(&vec![0; unlock_script_bytes]),
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
        let ids: std::collections::HashSet<_> = txs
            .iter()
            .map(|tx| tx.transaction.id().mined_id())
            .collect();
        for tx in txs {
            let id = tx.transaction.id().mined_id();
            let spent_mempool_outputs = tx
                .transaction
                .transaction()
                .spent_outpoints()
                .filter(|outpoint| ids.contains(&outpoint.hash))
                .collect();
            deps.add(id, spent_mempool_outputs);
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
        if matches!(kind, "mixed" | "mixed_packages" | "mixed_transparent") {
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
            let chained = matches!(kind, "packages" | "mixed_packages" | "mixed_transparent")
                && position % 6 != 0
                && position % 6 < 3;
            let spent = if chained {
                previous.into_iter().collect()
            } else {
                Vec::new()
            };
            let tx = if matches!(kind, "transparent" | "mixed_transparent") {
                // Payments, consolidations and payouts, with a spread of
                // fee rates. A chained child also spends its parent's output,
                // and the last child of each chain pays to bump the package.
                let (chain_inputs, outputs) =
                    TRANSPARENT_SHAPES[position % TRANSPARENT_SHAPES.len()];
                let fee_multiple = if chained && position % 6 == 2 {
                    4
                } else {
                    TRANSPARENT_FEE_MULTIPLES[position % TRANSPARENT_FEE_MULTIPLES.len()]
                };
                factory.transparent(chain_inputs, spent, outputs, fee_multiple)
            } else {
                factory.tx(
                    1,
                    if chained && position % 6 == 2 { 4 } else { 1 },
                    spent,
                    1,
                )
            };
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

/// A selector configuration compared by the benchmark.
#[derive(Clone, Copy, Debug)]
enum Mode {
    /// Weighted-random selection, as on main before the indexed selector.
    Random,
    /// The indexed selector before skip-and-continue: stop at the first
    /// package that does not fit, then pack from the next 100 entries.
    StopThenPack,
    /// The production selector.
    Production,
    /// Skip-and-continue that only gives up on misfits when nearly full by
    /// bytes, as briefly in production before the unconditional bound.
    SkipUntilNearlyFull,
    /// Skip-and-continue ranked by sigop-adjusted size.
    SigopAdjusted,
    /// Skip-and-continue ranked by the largest share of any block limit.
    ResourceAdjusted,
}

impl Mode {
    const ALL: [Self; 6] = [
        Self::Random,
        Self::StopThenPack,
        Self::Production,
        Self::SkipUntilNearlyFull,
        Self::SigopAdjusted,
        Self::ResourceAdjusted,
    ];

    fn name(self) -> &'static str {
        match self {
            Self::Random => "random",
            Self::StopThenPack => "stop_then_pack",
            Self::Production => "production",
            Self::SkipUntilNearlyFull => "skip_until_nearly_full",
            Self::SigopAdjusted => "sigop_adjusted",
            Self::ResourceAdjusted => "resource_adjusted",
        }
    }
}

/// Selects with the production entry point's limits.
fn select(
    mode: Mode,
    txs: Vec<VerifiedUnminedTx>,
    deps: TransactionDependencies,
    network: &Network,
    height: Height,
    miner: &MinerParams,
) -> Selection {
    let limits = limits(network, height, miner);
    let (score, misfit) = match mode {
        Mode::Random => {
            return Selection {
                txs: weighted_random(txs, &deps, limits, &mut rand_10::rng()),
                misfits: 0,
            }
        }
        Mode::Production => return run(txs, deps, limits),
        Mode::StopThenPack => (Score::Zip401Cost, Misfit::StopThenPack),
        Mode::SkipUntilNearlyFull => (Score::Zip401Cost, Misfit::SkipUntilNearlyFull),
        Mode::SigopAdjusted => (Score::SigopAdjustedSize, Misfit::Skip),
        Mode::ResourceAdjusted => (Score::ResourceAdjustedSize, Misfit::Skip),
    };
    variant(txs, deps, limits, score, misfit)
}

fn variant(
    txs: Vec<VerifiedUnminedTx>,
    deps: TransactionDependencies,
    limits: BlockTemplateLimits,
    score: Score,
    misfit: Misfit,
) -> Selection {
    let selection = VariantSelector::new(txs, deps, limits, score, misfit).select();
    Selection {
        txs: selection.txs,
        misfits: selection.misfits,
    }
}

#[derive(Clone, Copy)]
struct Stats {
    selected: usize,
    bytes: usize,
    fees: u64,
    remaining: usize,
    misfits: usize,
    sigops_left: u32,
    shielded_left: u32,
}
fn validate(
    selection: &Selection,
    fixture: &Fixture,
    network: &Network,
    height: Height,
    miner: &MinerParams,
) -> Stats {
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
        misfits: selection.misfits,
        sigops_left: limits.remaining_sigops,
        shielded_left: limits.remaining_shielded_cost,
    }
}

/// Benchmark eras: before NU7, and with the ZIP 218 shielded limits active,
/// as on Mainnet today.
const ERAS: [(&str, NetworkUpgrade); 2] = [
    ("pre_nu7", NetworkUpgrade::Nu6_3),
    ("nu7", NetworkUpgrade::Nu7),
];

fn context() -> (Network, Height, MinerParams, GetBlockTemplateChainInfo) {
    context_at(NetworkUpgrade::Nu6_3)
}

fn context_at(
    upgrade: NetworkUpgrade,
) -> (Network, Height, MinerParams, GetBlockTemplateChainInfo) {
    let network = Network::Mainnet;
    let height = upgrade.activation_height(&network).unwrap();
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
    let selection = select(Mode::Production, txs, deps, &network, height, &miner);
    validate(&selection, &fixture, &network, height, &miner);
    assert_eq!(selection.txs[0].1.transaction.id().mined_id(), parent_id);
    assert_eq!(
        selection.txs[1].1.transaction.id().mined_id(),
        independent_id
    );
}

#[test]
fn indexed_skips_packages_that_do_not_fit_beyond_any_scan_bound() {
    let (network, height, miner, _) = context();
    let mut factory = Factory::new();
    // More large, high-priority transactions than the old packing scan
    // visited, ahead of one small transaction that fits.
    let mut txs: Vec<_> = (0..110)
        .map(|_| factory.tx(64, 10, Vec::new(), 1))
        .collect();
    let small = factory.tx(1, 1, Vec::new(), 1);
    let small_size = small.transaction.size();
    let small_id = small.transaction.id().mined_id();
    assert!(txs[0].transaction.size() > small_size + SELECTION_STOP_BYTES);
    txs.push(small);
    let fixture = Fixture::from_transactions(txs);
    let limits_for_gap = || {
        let mut limits = limits(&network, height, &miner);
        limits.remaining_bytes = small_size + SELECTION_STOP_BYTES + 1;
        limits
    };

    let (txs, deps) = fixture.snapshot();
    let selection = run(txs, deps, limits_for_gap());
    validate(&selection, &fixture, &network, height, &miner);
    assert_eq!(ids(&selection), [small_id]);

    // The previous selector stopped at the first large transaction and had
    // too little space left to pack.
    let (txs, deps) = fixture.snapshot();
    let stopped = variant(
        txs,
        deps,
        limits_for_gap(),
        Score::Zip401Cost,
        Misfit::StopThenPack,
    );
    assert!(stopped.txs.is_empty());
}

#[test]
fn indexed_gives_up_after_consecutive_misfits_at_any_fill_level() {
    let (network, height, miner, _) = context();
    let mut factory = Factory::new();
    // The sigop budget is nearly used up while the block has plenty of room:
    // every transaction with sigops misfits, and only a sigop-free one fits.
    let small = factory.tx(1, 1, Vec::new(), 0);
    let small_id = small.transaction.id().mined_id();
    let sigop_heavy: Vec<_> = (0..MAX_CONSECUTIVE_MISFITS)
        .map(|_| factory.tx(1, 10, Vec::new(), 10))
        .collect();
    let limits_with_sigops_used = || {
        let mut limits = limits(&network, height, &miner);
        limits.remaining_sigops = 5;
        limits
    };
    assert!(sigop_heavy
        .iter()
        .all(|tx| tx.block_sigop_count() > limits_with_sigops_used().remaining_sigops));

    let run_with = |misfit: Option<Misfit>, sigop_heavy: &[VerifiedUnminedTx]| {
        let mut txs = sigop_heavy.to_vec();
        txs.push(small.clone());
        let fixture = Fixture::from_transactions(txs);
        let (txs, deps) = fixture.snapshot();
        let selection = match misfit {
            None => run(txs, deps, limits_with_sigops_used()),
            Some(misfit) => variant(
                txs,
                deps,
                limits_with_sigops_used(),
                Score::Zip401Cost,
                misfit,
            ),
        };
        ids(&selection)
    };

    // The sigop-free transaction ranks after every sigop-heavy one. It is
    // reached after one fewer than the bound, and not after the full bound,
    // even though most of the block is still empty.
    assert_eq!(run_with(None, &sigop_heavy[1..]), [small_id]);
    assert!(run_with(None, &sigop_heavy).is_empty());

    // Bounding only a nearly full template scans on to the end of the mempool.
    assert_eq!(
        run_with(Some(Misfit::SkipUntilNearlyFull), &sigop_heavy),
        [small_id]
    );
}

/// A parent and child that each fit a block but together exceed a block
/// limit form a package that can never fit. Ranked first by a high child fee,
/// it does not stop selection: the transactions after it are selected, and
/// so is the parent on its own. The child waits for a later block.
#[test]
fn indexed_selects_around_a_package_that_can_never_fit() {
    let (network, height, miner, _) = context_at(NetworkUpgrade::Nu7);
    let mut factory = Factory::new();
    // 170 Ironwood actions each: within the ZIP 218 limit of 330 alone, but
    // 340 together.
    let parent = factory.tx(170, 1, Vec::new(), 1);
    let parent_id = parent.transaction.id().mined_id();
    let child = factory.tx(170, 50, vec![OutPoint::from_usize(parent_id, 0)], 1);
    let child_id = child.transaction.id().mined_id();
    // More transparent transactions than the old packing pass scanned, ranked
    // between the package and the parent.
    let cover: Vec<_> = (0..150)
        .map(|_| factory.transparent(1, Vec::new(), 1, 10))
        .collect();
    let cover_ids: Vec<_> = cover
        .iter()
        .map(|tx| tx.transaction.id().mined_id())
        .collect();
    let mut txs = vec![parent, child];
    txs.extend(cover);
    let fixture = Fixture::from_transactions(txs);

    let (txs, deps) = fixture.snapshot();
    let selection = run(txs, deps, limits(&network, height, &miner));
    validate(&selection, &fixture, &network, height, &miner);
    let selected = ids(&selection);
    assert_eq!(selected.len(), cover_ids.len() + 1);
    assert!(cover_ids.iter().all(|id| selected.contains(id)));
    assert!(selected.contains(&parent_id));
    assert!(!selected.contains(&child_id));

    // Stop-then-pack ended at the package and packed only from the next
    // 100 entries, missing the parent, the child and the rest of the cover.
    let (txs, deps) = fixture.snapshot();
    let stopped = variant(
        txs,
        deps,
        limits(&network, height, &miner),
        Score::Zip401Cost,
        Misfit::StopThenPack,
    );
    assert!(stopped.txs.len() <= 100);
    assert!(!ids(&stopped).contains(&parent_id));
}

/// A child that ranks ahead of its parent is selected with it, at its
/// rescored priority, after a package that does not fit. The packing pass of
/// stop-then-pack visited the child before its parent was selected and never
/// came back to it.
#[test]
fn indexed_selects_a_child_ranked_before_its_parent_after_a_misfit() {
    let (network, height, miner, _) = context();
    let mut factory = Factory::new();
    let blocker = factory.tx(64, 20, Vec::new(), 1);
    let parent = factory.transparent(1, Vec::new(), 1, 1);
    let parent_id = parent.transaction.id().mined_id();
    let child = factory.transparent(0, vec![OutPoint::from_usize(parent_id, 0)], 1, 30);
    let child_id = child.transaction.id().mined_id();
    let remaining = 60_000;
    assert!(blocker.transaction.size() > remaining);
    let fixture = Fixture::from_transactions(vec![blocker, parent, child]);
    let limits_for_gap = || {
        let mut limits = limits(&network, height, &miner);
        limits.remaining_bytes = remaining;
        limits
    };

    let (txs, deps) = fixture.snapshot();
    let selection = run(txs, deps, limits_for_gap());
    validate(&selection, &fixture, &network, height, &miner);
    assert_eq!(ids(&selection), [parent_id, child_id]);

    let (txs, deps) = fixture.snapshot();
    let stopped = variant(
        txs,
        deps,
        limits_for_gap(),
        Score::Zip401Cost,
        Misfit::StopThenPack,
    );
    assert_eq!(ids(&stopped), [parent_id]);
}

#[test]
fn indexed_stops_at_three_kilobytes() {
    let (network, height, miner, _) = context();
    let mut factory = Factory::new();
    let first = factory.tx(0, 10, Vec::new(), 1);
    let first_size = first.transaction.size();
    let first_id = first.transaction.id().mined_id();
    let tail = factory.tx(0, 1, Vec::new(), 2);
    assert!(tail.transaction.size() < SELECTION_STOP_BYTES);
    let fixture = Fixture::from_transactions(vec![first, tail]);
    for remaining in [SELECTION_STOP_BYTES - 1, SELECTION_STOP_BYTES] {
        let (txs, deps) = fixture.snapshot();
        let mut limits = limits(&network, height, &miner);
        limits.remaining_bytes = remaining;
        assert!(run(txs, deps, limits).txs.is_empty());
    }
    let (txs, deps) = fixture.snapshot();
    let mut limits = limits(&network, height, &miner);
    limits.remaining_bytes = first_size + SELECTION_STOP_BYTES;
    let selection = run(txs, deps, limits);
    validate(&selection, &fixture, &network, height, &miner);
    assert_eq!(ids(&selection), [first_id]);
}

#[test]
fn indexed_stops_at_three_kilobytes_after_skipping() {
    let (network, height, miner, _) = context();
    let mut factory = Factory::new();
    let mut txs = vec![factory.tx(64, 10, Vec::new(), 1)];
    txs.extend((0..8).map(|_| factory.tx(1, 2, Vec::new(), 1)));
    let remaining = txs[1..]
        .iter()
        .map(|tx| tx.transaction.size())
        .sum::<usize>()
        + SELECTION_STOP_BYTES;
    assert!(txs[0].transaction.size() > remaining);
    let tail = factory.tx(0, 1, Vec::new(), 1);
    let tail_id = tail.transaction.id().mined_id();
    assert!(tail.transaction.size() < SELECTION_STOP_BYTES);
    txs.push(tail);
    let fixture = Fixture::from_transactions(txs);
    let (txs, deps) = fixture.snapshot();
    let mut limits = limits(&network, height, &miner);
    limits.remaining_bytes = remaining;
    // The large transaction is skipped, the eight mid-priority ones fill the
    // template to exactly the stop threshold, and the tail is never reached.
    let selection = run(txs, deps, limits);
    let stats = validate(&selection, &fixture, &network, height, &miner);
    assert_eq!(remaining - stats.bytes, SELECTION_STOP_BYTES);
    assert_eq!(selection.txs.len(), 8);
    assert!(!ids(&selection).contains(&tail_id));
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
            let selection = select(Mode::Production, txs, deps, &network, height, &miner);
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
        Mode::Production,
        vec![orphan, available],
        deps,
        &network,
        height,
        &miner,
    );
    assert_eq!(ids(&selection), [available_id]);
}

/// The variant selector with the production score and misfit rule selects
/// exactly what production selects, so benchmark differences come from the
/// variant settings alone. Every mode's output is valid in both eras.
#[test]
fn selection_variants_match_production_and_respect_limits() {
    for (era, upgrade) in ERAS {
        let (network, height, miner, _) = context_at(upgrade);
        for kind in [
            "uniform",
            "mixed",
            "packages",
            "mixed_packages",
            "transparent",
            "mixed_transparent",
        ] {
            // Large enough that every fixture overfills a block.
            let fixture = Fixture::full(kind, 12_000_000);
            let (txs, deps) = fixture.snapshot();
            let production = select(Mode::Production, txs, deps, &network, height, &miner);
            let (txs, deps) = fixture.snapshot();
            let mirrored = variant(
                txs,
                deps,
                limits(&network, height, &miner),
                Score::Zip401Cost,
                Misfit::Skip,
            );
            assert_eq!(ids(&production), ids(&mirrored), "{era} {kind}");

            for mode in Mode::ALL {
                let (txs, deps) = fixture.snapshot();
                let selection = if matches!(mode, Mode::Random) {
                    Selection {
                        txs: weighted_random(
                            txs,
                            &deps,
                            limits(&network, height, &miner),
                            &mut StdRng::seed_from_u64(317),
                        ),
                        misfits: 0,
                    }
                } else {
                    select(mode, txs, deps, &network, height, &miner)
                };
                validate(&selection, &fixture, &network, height, &miner);
            }
        }
    }
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
    let eras = std::env::var("ZAKURA_GBT_BENCH_ERAS").ok();
    for (era, upgrade) in ERAS.into_iter().filter(|(era, _)| {
        eras.as_deref()
            .is_none_or(|eras| eras.split(',').any(|selected| selected == *era))
    }) {
        let (network, height, miner, chain) = context_at(upgrade);
        let fixtures = std::env::var("ZAKURA_GBT_BENCH_FIXTURES").ok();
        let kinds = [
            "uniform",
            "mixed",
            "packages",
            "mixed_packages",
            "transparent",
            "mixed_transparent",
        ];
        for kind in kinds.into_iter().filter(|kind| {
            fixtures
                .as_deref()
                .is_none_or(|fixtures| fixtures.split(',').any(|fixture| fixture == *kind))
        }) {
            let fixture = Fixture::full(kind, cost_limit);
            // At full size the misfit bound can fire, so check here too that the
            // benchmark's skip rule selects exactly what production selects.
            let (txs, deps) = fixture.snapshot();
            let production = select(Mode::Production, txs, deps, &network, height, &miner);
            let (txs, deps) = fixture.snapshot();
            let mirrored = variant(
                txs,
                deps,
                limits(&network, height, &miner),
                Score::Zip401Cost,
                Misfit::Skip,
            );
            assert_eq!(ids(&production), ids(&mirrored), "{era} {kind}");
            let misfits = mirrored.misfits;
            println!(
                "gbt-check era={era} fixture={kind} production_matches_skip_variant=true misfits={misfits}"
            );
            for phase in ["selection", "template_json"] {
                let modes = Mode::ALL;
                let mut results: [Vec<(Duration, Stats)>; Mode::ALL.len()] = Default::default();
                // Sample 0 warms caches and is discarded.
                for sample in 0..=SAMPLES {
                    // Rotate and alternate the mode order, balancing position
                    // and warm-cache effects between modes.
                    let mut order: Vec<_> = (0..modes.len())
                        .map(|index| (index + sample) % modes.len())
                        .collect();
                    if sample % 2 == 1 {
                        order.reverse();
                    }
                    for index in order {
                        let mode = modes[index];
                        let mut elapsed = Duration::ZERO;
                        let selection = if phase == "selection" {
                            let (txs, deps) = fixture.snapshot();
                            let start = Instant::now();
                            let result =
                                black_box(select(mode, txs, deps, &network, height, &miner));
                            elapsed += start.elapsed();
                            result
                        } else {
                            let start = Instant::now();
                            let (txs, deps) = fixture.snapshot();
                            let result =
                                black_box(select(mode, txs, deps, &network, height, &miner));
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
                    let mode = mode.name();
                    result.sort_by_key(|(elapsed, _)| *elapsed);
                    let median = (result[SAMPLES / 2 - 1].0.as_nanos()
                        + result[SAMPLES / 2].0.as_nanos())
                        / 2;
                    let values = |extract: fn(Stats) -> u64| -> (u64, u64) {
                        let mut values: Vec<_> =
                            result.iter().map(|(_, stats)| extract(*stats)).collect();
                        values.sort();
                        (values[0], values[values.len() - 1])
                    };
                    let (selected_min, selected_max) =
                        values(|s| u64::try_from(s.selected).unwrap());
                    let (bytes_min, bytes_max) = values(|s| u64::try_from(s.bytes).unwrap());
                    let (fees_min, fees_max) = values(|s| s.fees);
                    let (remaining_min, remaining_max) =
                        values(|s| u64::try_from(s.remaining).unwrap());
                    let (_, misfits) = values(|s| u64::try_from(s.misfits).unwrap());
                    let (sigops_left, _) = values(|s| u64::from(s.sigops_left));
                    let (shielded_left, _) = values(|s| u64::from(s.shielded_left));
                    println!("gbt era={era} fixture={kind} cost_limit={cost_limit} pool_cost={} pool_txs={} phase={phase} mode={mode} median_ns={median} min_ns={} max_ns={} selected_min={selected_min} selected_max={selected_max} bytes_min={bytes_min} bytes_max={bytes_max} fees_min={fees_min} fees_max={fees_max} remaining_min={remaining_min} remaining_max={remaining_max} misfits={misfits} sigops_left={sigops_left} shielded_left={shielded_left}",
                    fixture.cost, fixture.pool.len(), result[0].0.as_nanos(), result[SAMPLES-1].0.as_nanos());
                }
            }
        }
    }
}
