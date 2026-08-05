//! Worst-case transaction verification, ported from Zebra so the two nodes can be
//! compared on identical inputs.
//!
//! The workload is built from the same mainnet test vectors both trees ship, packed to
//! `MAX_BLOCK_BYTES` under the ZIP 1271 action limits, and driven through the real
//! `transaction::Verifier` against a stub state. That makes it a like-for-like A/B against
//! `zebra-consensus/benches/worst_case_tx_verification.rs` — the numbers are directly
//! comparable, which the fleet measurements are not.
//!
//! # Thread counts
//!
//! Rayon's global pool and the Tokio runtime are both process-global, so a thread-count
//! sweep needs one process per point. Both are read from the environment rather than
//! baked into the case table:
//!
//! ```sh
//! ZAKURA_BENCH_RAYON_THREADS=1 cargo bench -p zakura-consensus --bench worst_case_tx_verification
//! ```
//!
//! This matters here specifically: all Zakura proof verification runs on the global rayon
//! pool, whose size comes from `sync.parallel_cpu_threads`, and `tower-batch-control`
//! sizes its batch queue from `rayon::current_num_threads()`. A node configured with one
//! rayon thread verifies serially *and* loses cross-batch pipelining.

// Disabled due to warnings in criterion macros.
#![allow(missing_docs)]
// Benchmark metadata is printed in machine-readable lines before each case runs.
#![allow(clippy::print_stdout)]
// Workload construction returns `Option` to mean "this case does not fit under
// the block size limit, skip it". A violated invariant *inside* construction is
// a different thing — a bug in the selection code — and must abort the run
// rather than be reported as a skipped case, so it panics.
#![allow(clippy::unwrap_in_result)]

use std::{
    cmp::Reverse,
    collections::{HashMap, HashSet},
    future::Future,
    io::Cursor,
    pin::Pin,
    sync::{Arc, Once},
    task::{Context, Poll},
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use futures::{stream::FuturesUnordered, StreamExt};
use tokio::sync::oneshot;
use tower::{buffer::Buffer, util::BoxService, Service, ServiceExt};

use zakura_chain::{
    block::{Block, Height, MAX_BLOCK_BYTES},
    parameters::Network,
    serialization::{DateTime32, ZcashDeserialize, ZcashSerialize},
    transaction::Transaction,
    transparent,
};
use zakura_consensus::{
    error::TransactionError,
    transaction::{self as tx, Request},
    BoxError,
};
use zakura_node_services::mempool;
use zakura_state as zs;
use zakura_test::vectors::MAINNET_BLOCKS;

const ALLOW_TRANSPARENT_PREVOUTS_WITHOUT_UTXOS: bool = false;
const SHIELDED_POOL_COUNT: usize = 3;
const SHIELDED_POOLS: [ShieldedPool; SHIELDED_POOL_COUNT] = [
    ShieldedPool::Sapling,
    ShieldedPool::Orchard,
    ShieldedPool::Sprout,
];
const MAINNET_BLOCK_HEADER_BYTES: usize = 1_487;
const CRITERION_SAMPLE_SIZE: usize = 10;
const ZIP1271_GLOBAL_SHIELDED_BUDGET: usize = 330;
const ZIP1271_ORCHARD_ACTION_LIMIT: usize = 330;
const ZIP1271_SAPLING_IO_LIMIT: usize = 300;

/// Thread count used for both process-global pools when the environment does not override
/// it. Matches the value Zebra's copy of this benchmark hardcodes, so an unconfigured run
/// in either tree is directly comparable.
const DEFAULT_BENCH_THREADS: usize = 4;

const RAYON_THREADS_VAR: &str = "ZAKURA_BENCH_RAYON_THREADS";
const TOKIO_THREADS_VAR: &str = "ZAKURA_BENCH_TOKIO_THREADS";

const BENCHMARK_CASES: &[BenchmarkCase] = &[
    BenchmarkCase {
        name: "full_orchard_limit",
        target: BenchmarkTarget::ActionLimits {
            action_limits: ActionLimits::zip1271(0, ZIP1271_ORCHARD_ACTION_LIMIT, 0),
        },
    },
    BenchmarkCase {
        name: "full_sapling_limit",
        target: BenchmarkTarget::ActionLimits {
            action_limits: ActionLimits::zip1271(ZIP1271_SAPLING_IO_LIMIT, 0, 0),
        },
    },
    BenchmarkCase {
        name: "current_light_wallet_worst_case",
        target: BenchmarkTarget::MaxSaplingOutputs,
    },
    BenchmarkCase {
        name: "current_light_wallet_trial_decrypt_worst_case",
        target: BenchmarkTarget::MaxSaplingOutputs,
    },
    BenchmarkCase {
        name: "current_full_node_worst_case",
        target: BenchmarkTarget::MaxSaplingSpends,
    },
];

#[derive(Clone, Debug)]
struct BenchmarkCase {
    name: &'static str,
    target: BenchmarkTarget,
}

#[derive(Clone, Copy, Debug)]
enum BenchmarkTarget {
    MaxSaplingOutputs,
    MaxSaplingSpends,
    ActionLimits { action_limits: ActionLimits },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ShieldedPool {
    Sapling,
    Orchard,
    Sprout,
}

#[derive(Clone, Debug)]
struct CandidateTx {
    transaction: Arc<Transaction>,
    serialized_len: usize,
    height: Height,
    time: DateTime<Utc>,
    counts: ActionCounts,
}

#[derive(Clone, Debug)]
struct Workload {
    requests: Vec<Request>,
    target_action_counts: ShieldedActionCounts,
    target_global_shielded_budget: Option<usize>,
    selection_strategy: &'static str,
    stats: WorkloadStats,
}

#[derive(Clone, Debug, Default)]
struct WorkloadStats {
    modeled_block_bytes: usize,
    serialized_bytes: usize,
    unique_transactions: usize,
    repeated_transactions: usize,
    action_counts: ActionCounts,
    verifier_checks: VerifierCheckCounts,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ActionCounts {
    transparent_inputs: usize,
    transparent_outputs: usize,
    sapling_spends: usize,
    sapling_outputs: usize,
    orchard_actions: usize,
    sprout_joinsplits: usize,
}

#[derive(Clone, Copy, Debug, Default)]
struct ShieldedActionCounts {
    counts: [usize; SHIELDED_POOL_COUNT],
}

#[derive(Clone, Copy, Debug)]
struct ActionLimits {
    pool_limits: ShieldedActionCounts,
    global_shielded_budget: usize,
}

#[derive(Clone, Copy, Debug, Default)]
struct VerifierCheckCounts {
    sapling_bundles: usize,
    orchard_bundles: usize,
    sprout_joinsplit_proofs: usize,
    sprout_signatures: usize,
}

#[derive(Clone, Copy, Debug, Default)]
struct CandidateLoadStats {
    skipped_coinbase: usize,
    skipped_unsupported_version: usize,
    skipped_transparent_prevouts: usize,
}

#[derive(Clone, Debug)]
struct BenchmarkSummary {
    case_name: &'static str,
    stats: WorkloadStats,
    sample_seconds: Vec<f64>,
}

type BenchmarkMempool =
    Buffer<BoxService<mempool::Request, mempool::Response, BoxError>, mempool::Request>;

type TxVerifier = Buffer<BoxService<Request, tx::Response, TransactionError>, Request>;

#[derive(Clone, Debug)]
struct BenchmarkState;

impl Service<zs::Request> for BenchmarkState {
    type Response = zs::Response;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: zs::Request) -> Self::Future {
        Box::pin(async move {
            match request {
                zs::Request::BestChainNextMedianTimePast => {
                    Ok(zs::Response::BestChainNextMedianTimePast(DateTime32::MIN))
                }
                zs::Request::CheckBestChainTipNullifiersAndAnchors(_) => {
                    Ok(zs::Response::ValidBestChainTipNullifiersAndAnchors)
                }
                unexpected => Err(format!(
                    "unexpected state request in tx verifier benchmark: {unexpected:?}"
                )
                .into()),
            }
        })
    }
}

fn worst_case_tx_verification(c: &mut Criterion) {
    init_rayon(rayon_threads());

    let (candidates, load_stats) = load_mainnet_candidates();
    println!(
        "worst_case_tx_verification: loaded {} mainnet candidate txs; skipped {} coinbase, {} unsupported-version, {} transparent-prevout txs",
        candidates.len(),
        load_stats.skipped_coinbase,
        load_stats.skipped_unsupported_version,
        load_stats.skipped_transparent_prevouts,
    );
    println!(
        "worst_case_tx_verification: mode=tx verifier repeated workload; max_block_bytes={}; limitation=uses repeated mainnet tx vectors, not a consensus-valid synthetic block",
        max_block_bytes(),
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(tokio_worker_threads())
        .enable_all()
        .build()
        .expect("tokio runtime should build for the benchmark");

    let mut benchmark_summaries = Vec::new();

    for case in BENCHMARK_CASES {
        let Some(workload) = build_workload(case, &candidates) else {
            println!(
                "worst_case_tx_verification: skipping case {}; no repeated mainnet candidate workload fit the requested shielded action mix under the max block size",
                case.name,
            );
            continue;
        };

        print_workload_metadata(case, &workload);

        let mut sample_seconds = Vec::new();

        c.bench_with_input(
            BenchmarkId::new("tx_verifier_repeated_workload", case.name),
            &workload.requests,
            |b, requests| {
                b.iter_custom(|iterations| {
                    let start = Instant::now();

                    for _ in 0..iterations {
                        let verified = runtime.block_on(async {
                            let verifier =
                                make_transaction_verifier(requests.len().saturating_add(1));
                            verify_requests(verifier, requests).await
                        });
                        black_box(verified);
                    }

                    let elapsed = start.elapsed();
                    let iterations =
                        u32::try_from(iterations).expect("benchmark iterations fit in u32");
                    sample_seconds.push(elapsed.as_secs_f64() / f64::from(iterations));

                    elapsed
                });
            },
        );

        benchmark_summaries.push(BenchmarkSummary {
            case_name: case.name,
            stats: workload.stats,
            sample_seconds,
        });
    }

    print_benchmark_summaries(&benchmark_summaries);
}

/// Size of the global rayon pool, which is where every proof verification actually runs.
fn rayon_threads() -> usize {
    env_threads(RAYON_THREADS_VAR)
}

/// Tokio worker count. Separate from the rayon pool, but the batch verifiers are driven
/// from it, so a sweep should usually move both together.
fn tokio_worker_threads() -> usize {
    env_threads(TOKIO_THREADS_VAR)
}

/// Reads a thread count from the environment, falling back to [`DEFAULT_BENCH_THREADS`].
///
/// Both pools are process-global and can only be configured once, so this is a per-process
/// setting rather than a per-case one: a sweep is a shell loop over benchmark invocations.
fn env_threads(name: &str) -> usize {
    let Ok(value) = std::env::var(name) else {
        return DEFAULT_BENCH_THREADS;
    };

    let threads: usize = value
        .parse()
        .unwrap_or_else(|error| panic!("{name} must be a thread count, got {value:?}: {error}"));

    assert!(threads > 0, "{name} must be at least 1");

    threads
}

fn init_rayon(threads: usize) {
    static INIT_RAYON: Once = Once::new();

    INIT_RAYON.call_once(|| {
        rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .build_global()
            .expect("rayon global thread pool should be initialized before proof verification");
    });
}

fn load_mainnet_candidates() -> (Vec<CandidateTx>, CandidateLoadStats) {
    let mut candidates = Vec::new();
    let mut stats = CandidateLoadStats::default();

    for (&height, &block_bytes) in MAINNET_BLOCKS.iter() {
        let block = Block::zcash_deserialize(Cursor::new(block_bytes))
            .expect("mainnet block test vector should deserialize");
        assert_eq!(
            block
                .header
                .zcash_serialize_to_vec()
                .expect("mainnet block header test vector should serialize")
                .len(),
            MAINNET_BLOCK_HEADER_BYTES,
            "benchmark block-size accounting should match mainnet serialized block headers",
        );

        for transaction in block.transactions {
            if transaction.is_coinbase() {
                stats.skipped_coinbase += 1;
                continue;
            }

            if transaction.version() < 4 {
                stats.skipped_unsupported_version += 1;
                continue;
            }

            let counts = ActionCounts::from_transaction(&transaction);

            if !ALLOW_TRANSPARENT_PREVOUTS_WITHOUT_UTXOS && counts.transparent_inputs > 0 {
                stats.skipped_transparent_prevouts += 1;
                continue;
            }

            candidates.push(CandidateTx {
                serialized_len: transaction
                    .zcash_serialize_to_vec()
                    .expect("transaction from a block vector should serialize")
                    .len(),
                transaction,
                height: Height(height),
                time: block.header.time,
                counts,
            });
        }
    }

    (candidates, stats)
}

fn build_workload(case: &BenchmarkCase, candidates: &[CandidateTx]) -> Option<Workload> {
    let (target_action_counts, target_global_shielded_budget, selected, selection_strategy) =
        match case.target {
            BenchmarkTarget::MaxSaplingOutputs => {
                let selected = select_sapling_output_heavy_workload(candidates)?;

                (
                    action_counts_for_selection(&selected, candidates).shielded_pool_actions(),
                    None,
                    selected,
                    "max_sapling_outputs_under_max_block_bytes",
                )
            }
            BenchmarkTarget::MaxSaplingSpends => {
                let selected = select_sapling_spend_heavy_workload(candidates)?;

                (
                    action_counts_for_selection(&selected, candidates).shielded_pool_actions(),
                    None,
                    selected,
                    "max_sapling_spends_under_max_block_bytes",
                )
            }
            BenchmarkTarget::ActionLimits { action_limits } => {
                let selected = select_candidates_for_limits(action_limits, candidates)?;
                let tx_bytes = selected_tx_bytes(&selected, candidates);
                let block_bytes = modeled_block_bytes(tx_bytes, selected.len());

                if block_bytes > max_block_bytes() {
                    return None;
                }

                (
                    action_limits.pool_limits,
                    Some(action_limits.global_shielded_budget),
                    selected,
                    "max_available_actions_under_zip1271_pool_and_global_limits_and_max_block_bytes",
                )
            }
        };

    let mut stats = WorkloadStats::default();
    let known_outpoint_hashes = Arc::new(HashSet::new());
    let known_utxos = Arc::new(HashMap::new());

    let requests = selected
        .iter()
        .map(|&index| {
            let candidate = &candidates[index];

            stats.serialized_bytes += candidate.serialized_len;
            stats.action_counts += candidate.counts;
            stats.verifier_checks += candidate.counts.verifier_check_counts();

            Request::Block {
                transaction_hash: candidate.transaction.hash(),
                transaction: candidate.transaction.clone(),
                known_outpoint_hashes: known_outpoint_hashes.clone(),
                known_utxos: known_utxos.clone(),
                height: candidate.height,
                time: candidate.time,
            }
        })
        .collect();

    stats.unique_transactions = selected.iter().copied().collect::<HashSet<_>>().len();
    stats.repeated_transactions = selected.len();
    stats.modeled_block_bytes = modeled_block_bytes(stats.serialized_bytes, selected.len());

    match case.target {
        BenchmarkTarget::ActionLimits { .. } => {
            let actual_counts = stats.action_counts.shielded_pool_actions();

            for pool in SHIELDED_POOLS {
                assert!(
                    actual_counts.action_count(pool) <= target_action_counts.action_count(pool),
                    "selected workload must not exceed the requested shielded pool action limits",
                );
            }
            assert!(
                stats.action_counts.global_shielded_budget()
                    <= target_global_shielded_budget
                        .expect("ZIP 1271 action-limit workloads have a global budget"),
                "selected workload must not exceed the requested global shielded budget",
            );
        }
        BenchmarkTarget::MaxSaplingOutputs | BenchmarkTarget::MaxSaplingSpends => {
            assert_eq!(
                stats.action_counts.shielded_pool_actions().counts,
                target_action_counts.counts,
                "selected workload must exactly match the requested shielded pool action mix",
            );
        }
    }
    assert!(
        stats.modeled_block_bytes <= max_block_bytes(),
        "selected workload must fit under the max block size",
    );

    Some(Workload {
        requests,
        target_action_counts,
        target_global_shielded_budget,
        selection_strategy,
        stats,
    })
}

fn select_sapling_output_heavy_workload(candidates: &[CandidateTx]) -> Option<Vec<usize>> {
    let mut best = None;

    for (index, candidate) in candidates.iter().enumerate() {
        if !candidate.has_only_pool_actions(ShieldedPool::Sapling)
            || candidate.counts.sapling_outputs == 0
        {
            continue;
        }

        let max_repeats = max_block_bytes() / candidate.serialized_len;

        for repeats in 1..=max_repeats {
            let tx_bytes = candidate.serialized_len * repeats;
            let block_bytes = modeled_block_bytes(tx_bytes, repeats);

            if block_bytes > max_block_bytes() {
                break;
            }

            let output_count = candidate.counts.sapling_outputs * repeats;
            let action_count = candidate.counts.action_count(ShieldedPool::Sapling) * repeats;

            if best
                .as_ref()
                .is_none_or(|(best_outputs, best_actions, best_bytes, _, _)| {
                    output_count > *best_outputs
                        || (output_count == *best_outputs && action_count > *best_actions)
                        || (output_count == *best_outputs
                            && action_count == *best_actions
                            && block_bytes > *best_bytes)
                })
            {
                best = Some((output_count, action_count, block_bytes, index, repeats));
            }
        }
    }

    let (_, _, _, index, repeats) = best?;

    Some(vec![index; repeats])
}

fn select_sapling_spend_heavy_workload(candidates: &[CandidateTx]) -> Option<Vec<usize>> {
    let mut best = None;

    for (index, candidate) in candidates.iter().enumerate() {
        if !candidate.has_only_pool_actions(ShieldedPool::Sapling)
            || candidate.counts.sapling_spends == 0
        {
            continue;
        }

        let max_repeats = max_block_bytes() / candidate.serialized_len;

        for repeats in 1..=max_repeats {
            let tx_bytes = candidate.serialized_len * repeats;
            let block_bytes = modeled_block_bytes(tx_bytes, repeats);

            if block_bytes > max_block_bytes() {
                break;
            }

            let spend_count = candidate.counts.sapling_spends * repeats;
            let action_count = candidate.counts.action_count(ShieldedPool::Sapling) * repeats;

            if best
                .as_ref()
                .is_none_or(|(best_spends, best_actions, best_bytes, _, _)| {
                    spend_count > *best_spends
                        || (spend_count == *best_spends && action_count > *best_actions)
                        || (spend_count == *best_spends
                            && action_count == *best_actions
                            && block_bytes > *best_bytes)
                })
            {
                best = Some((spend_count, action_count, block_bytes, index, repeats));
            }
        }
    }

    let (_, _, _, index, repeats) = best?;

    Some(vec![index; repeats])
}

fn select_candidates_for_limits(
    action_limits: ActionLimits,
    candidates: &[CandidateTx],
) -> Option<Vec<usize>> {
    let mut selected = Vec::new();
    let mut selected_counts = ActionCounts::default();

    for pool in SHIELDED_POOLS {
        let remaining_global_budget = action_limits
            .global_shielded_budget
            .saturating_sub(selected_counts.global_shielded_budget());
        let action_limit = action_limits
            .pool_limits
            .action_count(pool)
            .min(remaining_global_budget / pool.global_budget_per_action());

        if action_limit == 0 {
            continue;
        }

        let mut matching_indices: Vec<_> = candidates
            .iter()
            .enumerate()
            .filter(|(_, candidate)| candidate.has_only_pool_actions(pool))
            .map(|(index, _)| index)
            .collect();

        matching_indices.sort_by_key(|index| Reverse(candidates[*index].pool_score(pool)));

        let pool_selected = (1..=action_limit).rev().find_map(|target_actions| {
            select_pool_candidates_for_limits(pool, target_actions, &matching_indices, candidates)
        })?;

        for index in &pool_selected {
            selected_counts += candidates[*index].counts;
        }

        selected.extend(pool_selected);
    }

    Some(selected)
}

fn select_pool_candidates_for_limits(
    pool: ShieldedPool,
    target_actions: usize,
    matching_indices: &[usize],
    candidates: &[CandidateTx],
) -> Option<Vec<usize>> {
    let mut previous_selection = vec![None; target_actions.saturating_add(1)];
    previous_selection[0] = Some((0, 0, 0, usize::MAX));

    for selected_actions in 0..=target_actions {
        let Some((selected_score, selected_bytes, _, _)) = previous_selection[selected_actions]
        else {
            continue;
        };

        for &index in matching_indices {
            let candidate = &candidates[index];
            let next_actions = selected_actions.saturating_add(candidate.counts.action_count(pool));
            let next_score = selected_score + candidate.limit_score(pool);
            let next_bytes = selected_bytes + candidate.serialized_len;

            if next_actions <= target_actions
                && previous_selection[next_actions].is_none_or(|(best_score, best_bytes, _, _)| {
                    next_score > best_score || (next_score == best_score && next_bytes < best_bytes)
                })
            {
                previous_selection[next_actions] =
                    Some((next_score, next_bytes, selected_actions, index));
            }
        }
    }

    previous_selection[target_actions]?;

    let mut selected = Vec::new();
    let mut remaining_actions = target_actions;

    while remaining_actions > 0 {
        let (_, _, previous_actions, index) = previous_selection[remaining_actions]?;

        selected.push(index);
        remaining_actions = previous_actions;
    }

    Some(selected)
}

fn selected_tx_bytes(selected: &[usize], candidates: &[CandidateTx]) -> usize {
    selected
        .iter()
        .map(|&index| candidates[index].serialized_len)
        .sum()
}

fn action_counts_for_selection(selected: &[usize], candidates: &[CandidateTx]) -> ActionCounts {
    let mut action_counts = ActionCounts::default();

    for &index in selected {
        action_counts += candidates[index].counts;
    }

    action_counts
}

fn modeled_block_bytes(tx_bytes: usize, tx_count: usize) -> usize {
    MAINNET_BLOCK_HEADER_BYTES + compact_size_len(tx_count) + tx_bytes
}

fn compact_size_len(count: usize) -> usize {
    match count {
        0..=252 => 1,
        253..=0xffff => 3,
        0x1_0000..=0xffff_ffff => 5,
        _ => 9,
    }
}

fn print_workload_metadata(case: &BenchmarkCase, workload: &Workload) {
    let requested_actions = workload.target_action_counts;
    let actual_actions = workload.stats.action_counts.shielded_pool_actions();
    let actual_total_actions = actual_actions.total();
    let actual_global_shielded_budget = workload.stats.action_counts.global_shielded_budget();
    let stats = &workload.stats;

    println!(
        "worst_case_tx_verification: case={} mode=tx verifier repeated workload target_block_bytes={} actual_block_bytes={} actual_tx_bytes={} block_fill_percent={:.2} block_bytes_remaining={} actual_shielded_pool_actions={} actual_global_shielded_budget={} unique_txs={} repeated_txs={} rayon_threads={} tokio_worker_threads={} transparent_prevouts_allowed={}",
        case.name,
        max_block_bytes(),
        stats.modeled_block_bytes,
        stats.serialized_bytes,
        percent(stats.modeled_block_bytes, max_block_bytes()),
        max_block_bytes() - stats.modeled_block_bytes,
        actual_total_actions,
        actual_global_shielded_budget,
        stats.unique_transactions,
        stats.repeated_transactions,
        rayon_threads(),
        tokio_worker_threads(),
        ALLOW_TRANSPARENT_PREVOUTS_WITHOUT_UTXOS,
    );
    println!(
        "worst_case_tx_verification: case={} workload_source=mainnet_test_vectors workload_validity=repeated_txs_not_consensus_block selection_strategy={}",
        case.name,
        workload.selection_strategy,
    );
    println!(
        "worst_case_tx_verification: case={} requested_pool_actions {}",
        case.name,
        pool_action_fields(requested_actions),
    );
    if let Some(requested_global_shielded_budget) = workload.target_global_shielded_budget {
        println!(
            "worst_case_tx_verification: case={} requested_global_shielded_budget={}",
            case.name, requested_global_shielded_budget,
        );
    }
    match case.target {
        BenchmarkTarget::MaxSaplingOutputs => {
            println!(
                "worst_case_tx_verification: case={} requested_workload_goal sapling_outputs=max",
                case.name,
            );
        }
        BenchmarkTarget::MaxSaplingSpends => {
            println!(
                "worst_case_tx_verification: case={} requested_workload_goal sapling_spends=max",
                case.name,
            );
        }
        BenchmarkTarget::ActionLimits { .. } => {}
    }
    println!(
        "worst_case_tx_verification: case={} actual_pool_actions {}",
        case.name,
        pool_action_percent_fields(actual_actions, actual_total_actions),
    );
    println!(
        "worst_case_tx_verification: case={} raw_actions transparent_inputs={} transparent_outputs={} sapling_spends={} sapling_outputs={} orchard_actions={} sprout_joinsplits={}",
        case.name,
        stats.action_counts.transparent_inputs,
        stats.action_counts.transparent_outputs,
        stats.action_counts.sapling_spends,
        stats.action_counts.sapling_outputs,
        stats.action_counts.orchard_actions,
        stats.action_counts.sprout_joinsplits,
    );
    println!(
        "worst_case_tx_verification: case={} verifier_checks sapling_bundles={} orchard_bundles={} sprout_joinsplit_proofs={} sprout_signatures={}",
        case.name,
        stats.verifier_checks.sapling_bundles,
        stats.verifier_checks.orchard_bundles,
        stats.verifier_checks.sprout_joinsplit_proofs,
        stats.verifier_checks.sprout_signatures,
    );
}

fn print_benchmark_summaries(summaries: &[BenchmarkSummary]) {
    println!("worst_case_tx_verification_summary_csv:");
    println!(
        "case,total_bytes,tx_bytes,block_fill_percent,repeated_txs,unique_txs,transparent_inputs,transparent_outputs,sapling_spends,sapling_outputs,sapling_actions,orchard_actions,sprout_joinsplits,total_shielded_actions,global_shielded_budget,sapling_bundles,orchard_bundles,sprout_joinsplit_proofs,sprout_signatures,mean_ms,stddev_ms,time_ms"
    );

    for summary in summaries {
        let Some((mean_seconds, stddev_seconds)) = mean_and_stddev(summary_samples(summary)) else {
            println!(
                "{},{},{},{:.2},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},not_run,not_run,not_run",
                summary.case_name,
                summary.stats.modeled_block_bytes,
                summary.stats.serialized_bytes,
                percent(summary.stats.modeled_block_bytes, max_block_bytes()),
                summary.stats.repeated_transactions,
                summary.stats.unique_transactions,
                summary.stats.action_counts.transparent_inputs,
                summary.stats.action_counts.transparent_outputs,
                summary.stats.action_counts.sapling_spends,
                summary.stats.action_counts.sapling_outputs,
                summary
                    .stats
                    .action_counts
                    .action_count(ShieldedPool::Sapling),
                summary.stats.action_counts.orchard_actions,
                summary.stats.action_counts.sprout_joinsplits,
                summary.stats.action_counts.shielded_pool_actions().total(),
                summary.stats.action_counts.global_shielded_budget(),
                summary.stats.verifier_checks.sapling_bundles,
                summary.stats.verifier_checks.orchard_bundles,
                summary.stats.verifier_checks.sprout_joinsplit_proofs,
                summary.stats.verifier_checks.sprout_signatures,
            );
            continue;
        };

        let mean_ms = mean_seconds * 1_000.0;
        let stddev_ms = stddev_seconds * 1_000.0;

        println!(
            "{},{},{},{:.2},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{:.3},{:.3},{:.3} +/- {:.3} ms",
            summary.case_name,
            summary.stats.modeled_block_bytes,
            summary.stats.serialized_bytes,
            percent(summary.stats.modeled_block_bytes, max_block_bytes()),
            summary.stats.repeated_transactions,
            summary.stats.unique_transactions,
            summary.stats.action_counts.transparent_inputs,
            summary.stats.action_counts.transparent_outputs,
            summary.stats.action_counts.sapling_spends,
            summary.stats.action_counts.sapling_outputs,
            summary.stats.action_counts.action_count(ShieldedPool::Sapling),
            summary.stats.action_counts.orchard_actions,
            summary.stats.action_counts.sprout_joinsplits,
            summary.stats.action_counts.shielded_pool_actions().total(),
            summary.stats.action_counts.global_shielded_budget(),
            summary.stats.verifier_checks.sapling_bundles,
            summary.stats.verifier_checks.orchard_bundles,
            summary.stats.verifier_checks.sprout_joinsplit_proofs,
            summary.stats.verifier_checks.sprout_signatures,
            mean_ms,
            stddev_ms,
            mean_ms,
            stddev_ms,
        );
    }
}

fn summary_samples(summary: &BenchmarkSummary) -> &[f64] {
    let sample_count = summary.sample_seconds.len();
    let start = sample_count.saturating_sub(CRITERION_SAMPLE_SIZE);

    &summary.sample_seconds[start..]
}

fn mean_and_stddev(samples: &[f64]) -> Option<(f64, f64)> {
    if samples.is_empty() {
        return None;
    }

    let sample_count = u32::try_from(samples.len()).expect("benchmark sample count fits in u32");
    let mean = samples.iter().sum::<f64>() / f64::from(sample_count);
    let variance = if samples.len() > 1 {
        samples
            .iter()
            .map(|sample| {
                let difference = sample - mean;
                difference * difference
            })
            .sum::<f64>()
            / f64::from(sample_count - 1)
    } else {
        0.0
    };

    Some((mean, variance.sqrt()))
}

fn pool_action_fields(counts: ShieldedActionCounts) -> String {
    pool_fields(|pool| format!("{}={}", pool.name(), counts.action_count(pool)))
}

fn pool_action_percent_fields(counts: ShieldedActionCounts, total: usize) -> String {
    pool_fields(|pool| {
        let actions = counts.action_count(pool);

        format!(
            "{}={} ({:.2}%)",
            pool.name(),
            actions,
            percent(actions, total)
        )
    })
}

fn pool_fields(field: impl Fn(ShieldedPool) -> String) -> String {
    SHIELDED_POOLS.map(field).join(" ")
}

fn percent(count: usize, total: usize) -> f64 {
    if total == 0 {
        0.0
    } else {
        let count = u32::try_from(count).expect("benchmark action counts fit in u32");
        let total = u32::try_from(total).expect("benchmark action counts fit in u32");

        f64::from(count) * 100.0 / f64::from(total)
    }
}

fn max_block_bytes() -> usize {
    usize::try_from(MAX_BLOCK_BYTES).expect("Zcash max block bytes fit in usize")
}

fn make_transaction_verifier(buffer_bound: usize) -> TxVerifier {
    let verifier = tx::Verifier::new(&Network::Mainnet, BenchmarkState, closed_mempool_setup_rx());

    Buffer::new(BoxService::new(verifier), buffer_bound)
}

fn closed_mempool_setup_rx() -> oneshot::Receiver<BenchmarkMempool> {
    oneshot::channel().1
}

async fn verify_requests(verifier: TxVerifier, requests: &[Request]) -> usize {
    let mut futures = FuturesUnordered::new();

    for request in requests.iter().cloned() {
        let mut verifier = verifier.clone();

        futures.push(async move {
            verifier
                .ready()
                .await
                .expect("transaction verifier should always be ready")
                .call(request)
                .await
        });
    }

    let mut verified = 0;

    while let Some(result) = futures.next().await {
        result.expect("benchmark transaction should verify successfully");
        verified += 1;
    }

    assert_eq!(
        verified,
        requests.len(),
        "all benchmark transactions should be verified",
    );

    verified
}

impl CandidateTx {
    fn has_only_pool_actions(&self, pool: ShieldedPool) -> bool {
        self.counts.action_count(pool) > 0
            && SHIELDED_POOLS
                .iter()
                .copied()
                .filter(|candidate_pool| *candidate_pool != pool)
                .all(|candidate_pool| self.counts.action_count(candidate_pool) == 0)
    }

    fn pool_score(&self, pool: ShieldedPool) -> (usize, usize, usize, usize) {
        (
            self.counts.action_count(pool),
            self.serialized_len,
            self.counts.sapling_spends,
            self.counts.sapling_outputs,
        )
    }

    fn limit_score(&self, pool: ShieldedPool) -> usize {
        match pool {
            ShieldedPool::Sapling => self.counts.sapling_spends,
            ShieldedPool::Orchard | ShieldedPool::Sprout => self.counts.action_count(pool),
        }
    }
}

impl ShieldedPool {
    const fn index(self) -> usize {
        match self {
            ShieldedPool::Sapling => 0,
            ShieldedPool::Orchard => 1,
            ShieldedPool::Sprout => 2,
        }
    }

    const fn name(self) -> &'static str {
        match self {
            ShieldedPool::Sapling => "sapling",
            ShieldedPool::Orchard => "orchard",
            ShieldedPool::Sprout => "sprout",
        }
    }

    const fn global_budget_per_action(self) -> usize {
        match self {
            ShieldedPool::Sapling | ShieldedPool::Orchard => 1,
            ShieldedPool::Sprout => 2,
        }
    }
}

impl ActionCounts {
    fn from_transaction(transaction: &Transaction) -> Self {
        Self {
            transparent_inputs: transaction
                .inputs()
                .iter()
                .filter(|input| matches!(input, transparent::Input::PrevOut { .. }))
                .count(),
            transparent_outputs: transaction.outputs().len(),
            sapling_spends: transaction.sapling_spends_per_anchor().count(),
            sapling_outputs: transaction.sapling_outputs().count(),
            orchard_actions: transaction.orchard_actions().count(),
            sprout_joinsplits: transaction.joinsplit_count(),
        }
    }

    fn shielded_pool_actions(&self) -> ShieldedActionCounts {
        ShieldedActionCounts {
            counts: SHIELDED_POOLS.map(|pool| self.action_count(pool)),
        }
    }

    fn action_count(&self, pool: ShieldedPool) -> usize {
        match pool {
            ShieldedPool::Sapling => self.sapling_spends + self.sapling_outputs,
            ShieldedPool::Orchard => self.orchard_actions,
            ShieldedPool::Sprout => self.sprout_joinsplits,
        }
    }

    fn global_shielded_budget(&self) -> usize {
        self.sapling_spends
            + self.sapling_outputs
            + self.orchard_actions
            + self.sprout_joinsplits * ShieldedPool::Sprout.global_budget_per_action()
    }

    fn verifier_check_counts(&self) -> VerifierCheckCounts {
        VerifierCheckCounts {
            sapling_bundles: usize::from(self.sapling_spends + self.sapling_outputs > 0),
            orchard_bundles: usize::from(self.orchard_actions > 0),
            sprout_joinsplit_proofs: self.sprout_joinsplits,
            sprout_signatures: usize::from(self.sprout_joinsplits > 0),
        }
    }
}

impl ShieldedActionCounts {
    const fn new(sapling: usize, orchard: usize, sprout: usize) -> Self {
        Self {
            counts: [sapling, orchard, sprout],
        }
    }

    fn total(&self) -> usize {
        self.counts.iter().sum()
    }

    fn action_count(&self, pool: ShieldedPool) -> usize {
        self.counts[pool.index()]
    }
}

impl ActionLimits {
    const fn zip1271(sapling: usize, orchard: usize, sprout: usize) -> Self {
        Self {
            pool_limits: ShieldedActionCounts::new(sapling, orchard, sprout),
            global_shielded_budget: ZIP1271_GLOBAL_SHIELDED_BUDGET,
        }
    }
}

impl std::ops::AddAssign for ActionCounts {
    fn add_assign(&mut self, rhs: Self) {
        self.transparent_inputs += rhs.transparent_inputs;
        self.transparent_outputs += rhs.transparent_outputs;
        self.sapling_spends += rhs.sapling_spends;
        self.sapling_outputs += rhs.sapling_outputs;
        self.orchard_actions += rhs.orchard_actions;
        self.sprout_joinsplits += rhs.sprout_joinsplits;
    }
}

impl std::ops::AddAssign for VerifierCheckCounts {
    fn add_assign(&mut self, rhs: Self) {
        self.sapling_bundles += rhs.sapling_bundles;
        self.orchard_bundles += rhs.orchard_bundles;
        self.sprout_joinsplit_proofs += rhs.sprout_joinsplit_proofs;
        self.sprout_signatures += rhs.sprout_signatures;
    }
}

criterion_group!(
    name = benches;
    config = Criterion::default()
        .noise_threshold(0.05)
        .sample_size(CRITERION_SAMPLE_SIZE)
        .measurement_time(Duration::from_secs(30));
    targets = worst_case_tx_verification
);
criterion_main!(benches);
