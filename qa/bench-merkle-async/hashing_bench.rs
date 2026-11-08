//! Benchmark-only harness for the Merkle parallelism / async hashing A/B/C comparison.
//!
//! Copied unchanged into `crates/zakura-consensus/src/block/tests/hashing_bench.rs` at each arm,
//! next to that arm's `hashing_bench/arm.rs` shim. Run as an ignored release test; see README.md.

use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use rayon::prelude::*;
use zakura_chain::{
    block::{merkle::Root, Block, Height},
    parameters::{Network, NetworkUpgrade},
    serialization::{ZcashDeserializeInto, ZcashSerialize},
    transaction::{self, Transaction},
};
use zakura_node_services::block_memory::BlockHandle;

mod arm;

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn median(v: &mut [f64]) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).expect("timings are finite"));
    v[v.len() / 2]
}

fn leaves(count: usize) -> Vec<transaction::Hash> {
    (0..count as u64)
        .map(|i| {
            let mut leaf = [0u8; 32];
            leaf[..8].copy_from_slice(&i.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_le_bytes());
            leaf[8..16].copy_from_slice(&i.to_le_bytes());
            transaction::Hash(leaf)
        })
        .collect()
}

/// Warm-data Merkle root construction from precomputed transaction hashes.
fn merkle_micro(label: &str) {
    let samples = env_usize("BENCH_MERKLE_SAMPLES", 15);
    for count in [
        1usize, 2, 10, 63, 64, 100, 300, 511, 512, 513, 1023, 1024, 1025, 2000, 3001, 4096, 5000,
        8192,
    ] {
        let hashes = leaves(count);
        let serial: Root = hashes.iter().copied().collect();
        let root = arm::merkle_root(&hashes);
        assert_eq!(root, serial, "{count} leaves: arm root differs from serial");
        // Size inner loops to ~5 ms so timer resolution does not matter.
        let start = Instant::now();
        let mut inner = 0usize;
        while start.elapsed() < Duration::from_millis(5) {
            std::hint::black_box(arm::merkle_root(std::hint::black_box(&hashes)));
            inner += 1;
        }
        let mut per_call = Vec::with_capacity(samples);
        for _ in 0..samples {
            let start = Instant::now();
            for _ in 0..inner {
                std::hint::black_box(arm::merkle_root(std::hint::black_box(&hashes)));
            }
            per_call.push(start.elapsed().as_nanos() as f64 / inner as f64);
        }
        let (lo, hi) = (
            per_call.iter().cloned().fold(f64::MAX, f64::min),
            per_call.iter().cloned().fold(0.0, f64::max),
        );
        println!(
            "MERKLE,{label},{count},{:.0},{lo:.0},{hi:.0},{inner},{}",
            median(&mut per_call),
            hex::encode(&root.0[..8])
        );
    }
}

/// Builds a mainnet-like block from vector transactions, unique by expiry height, with a
/// matching Merkle root. `shielded` selects whether shielded transactions may be included.
fn fixture_block(target_bytes: usize, max_txs: usize, shielded: bool) -> Block {
    let network = Network::Mainnet;
    let mut blocks: Vec<Block> = zakura_test::vectors::MAINNET_BLOCKS
        .values()
        .map(|bytes| bytes.zcash_deserialize_into().expect("vector deserializes"))
        .collect();
    let template = blocks.pop().expect("vectors are not empty");
    let nu = NetworkUpgrade::current(&network, template.coinbase_height().expect("height"));
    let mut candidates: Vec<Transaction> = blocks
        .iter()
        .chain(std::iter::once(&template))
        .flat_map(|b| b.transactions.iter())
        .filter(|tx| !tx.is_coinbase() && tx.version() >= 3)
        .filter(|tx| tx.network_upgrade().is_none_or(|tx_nu| tx_nu == nu))
        .filter(|tx| shielded || !tx.has_shielded_data())
        .map(|tx| tx.as_ref().clone())
        .collect();
    assert!(!candidates.is_empty(), "vector candidates exist");
    if shielded {
        // Put shielded transactions first so a mixed block really is mixed.
        candidates.sort_by_key(|tx| !tx.has_shielded_data());
    }
    let mut transactions = vec![template.transactions[0].clone()];
    let mut size = template.header.zcash_serialized_size() + transactions[0].zcash_serialized_size();
    let mut i = 0u32;
    while transactions.len() < max_txs {
        let mut tx = candidates[i as usize % candidates.len()].clone();
        *tx.expiry_height_mut() = Height(4_000_000 + i);
        let len = tx.zcash_serialized_size();
        if size + len > target_bytes {
            break;
        }
        size += len;
        transactions.push(Arc::new(tx));
        i += 1;
    }
    let root: Root = transactions.iter().map(|tx| tx.hash()).collect();
    let mut header = template.header.as_ref().clone();
    header.merkle_root = root;
    Block {
        header: Arc::new(header),
        transactions,
    }
}

struct Case {
    name: &'static str,
    raw: Block,
    block: BlockHandle,
    shielded_txs: usize,
}

fn cases() -> Vec<Case> {
    [
        ("transparent_small_10tx", 2_000_000, 10, false),
        ("transparent_63tx", 2_000_000, 63, false),
        ("transparent_64tx", 2_000_000, 64, false),
        ("transparent_250kB", 250_000, usize::MAX, false),
        ("mixed_250kB", 250_000, usize::MAX, true),
        ("transparent_1.9MB", 1_900_000, usize::MAX, false),
        ("mixed_1.9MB", 1_900_000, usize::MAX, true),
    ]
    .into_iter()
    .map(|(name, bytes, max, shielded)| {
        let block = fixture_block(bytes, max, shielded);
        let shielded_txs = block.transactions.iter().filter(|t| t.has_shielded_data()).count();
        Case {
            name,
            block: Arc::new(block.clone()).into(),
            raw: block,
            shielded_txs,
        }
    })
    .collect()
}

/// Keeps every global Rayon worker busy with short synthetic jobs until stopped.
fn start_rayon_contention(stop: Arc<AtomicBool>) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        while !stop.load(Ordering::Relaxed) {
            (0..rayon::current_num_threads() * 4)
                .into_par_iter()
                .for_each(|_| {
                    let start = Instant::now();
                    let mut x = 0u64;
                    while start.elapsed() < Duration::from_micros(2_000) {
                        x = std::hint::black_box(x.wrapping_mul(31).wrapping_add(7));
                    }
                });
        }
    })
}

/// Times one production hashing phase on a 1-worker multi-thread Tokio runtime, with a 1 ms
/// ticker task on the same runtime. Returns the phase latency, the ticker's largest gap
/// between wakeups around the phase (µs), and its wakeups during the phase.
fn timed_phase(rt: &tokio::runtime::Runtime, network: &Network, block: &BlockHandle) -> (f64, f64, u64) {
    rt.block_on(async {
        let max_gap_us = Arc::new(AtomicU64::new(0));
        let ticks = Arc::new(AtomicU64::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let ticker = {
            let (max_gap_us, ticks, stop) = (max_gap_us.clone(), ticks.clone(), stop.clone());
            tokio::spawn(async move {
                let mut last = Instant::now();
                while !stop.load(Ordering::Relaxed) {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    let now = Instant::now();
                    max_gap_us.fetch_max((now - last).as_micros() as u64, Ordering::Relaxed);
                    ticks.fetch_add(1, Ordering::Relaxed);
                    last = now;
                }
            })
        };
        // Let the ticker start and reset its counters so they cover only the phase.
        tokio::time::sleep(Duration::from_millis(3)).await;
        max_gap_us.store(0, Ordering::Relaxed);
        ticks.store(0, Ordering::Relaxed);
        let (network, block) = (network.clone(), block.clone());
        let start = Instant::now();
        let result = tokio::spawn(async move { arm::phase(network, block).await })
            .await
            .expect("phase does not panic");
        let elapsed = start.elapsed();
        let ticked = ticks.load(Ordering::Relaxed);
        // A tick held off by the phase records its gap only once it runs again.
        tokio::time::sleep(Duration::from_millis(5)).await;
        let gap = max_gap_us.load(Ordering::Relaxed) as f64;
        stop.store(true, Ordering::Relaxed);
        ticker.await.expect("ticker does not panic");
        result.expect("fixture blocks are valid");
        (elapsed.as_secs_f64() * 1e6, gap, ticked)
    })
}

fn phase_bench(label: &str) {
    let samples = env_usize("BENCH_PHASE_SAMPLES", 11);
    let network = Network::Mainnet;
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(env_usize("BENCH_TOKIO_WORKERS", 1))
        .max_blocking_threads(4)
        .enable_all()
        .build()
        .expect("runtime builds");
    let cases = cases();

    for case in &cases {
        // Output and error-verdict equivalence: hashes, auth root, and verdicts for a corrupted
        // root and a duplicated transaction.
        let (hashes, auth) = rt
            .block_on(arm::phase(network.clone(), case.block.clone()))
            .expect("fixture is valid");
        let mut digest = blake2b_simd::Params::new().hash_length(16).to_state();
        hashes.iter().for_each(|h| {
            digest.update(&h.0);
        });
        let mut bad_root = case.raw.clone();
        Arc::make_mut(&mut bad_root.header).merkle_root = Root([7; 32]);
        let bad_root_verdict = rt
            .block_on(arm::phase(network.clone(), Arc::new(bad_root).into()))
            .err();
        let mut dup = case.raw.clone();
        let last = dup.transactions.last().expect("non-empty").clone();
        dup.transactions.push(last);
        // Odd-length duplicate of the last leaf keeps the root (CVE-2012-2459) only for odd
        // counts; set the header root to the duplicated tree so the duplicate check decides.
        let dup_root: Root = dup.transactions.iter().map(|t| t.hash()).collect();
        Arc::make_mut(&mut dup.header).merkle_root = dup_root;
        let dup_verdict = rt
            .block_on(arm::phase(network.clone(), Arc::new(dup).into()))
            .err();
        println!(
            "VERIFY,{label},{},{}tx,{}shielded,{}bytes,txids={},auth_root={},bad_root={:?},dup={:?}",
            case.name,
            hashes.len(),
            case.shielded_txs,
            case.raw.zcash_serialized_size(),
            digest.finalize().to_hex(),
            hex::encode(<[u8; 32]>::from(auth.root)),
            bad_root_verdict.map(|e| format!("{e:?}").split(' ').next().unwrap_or("").to_string()),
            dup_verdict.map(|e| format!("{e:?}")),
        );
    }

    // Where the phase's CPU goes, timed with production functions on a plain thread.
    for case in &cases {
        let mut ids = Vec::new();
        let mut merkle = Vec::new();
        let mut auth = Vec::new();
        for _ in 0..samples {
            let start = Instant::now();
            let hashes = std::hint::black_box(case.block.view().transaction_hashes());
            ids.push(start.elapsed().as_secs_f64() * 1e6);
            let start = Instant::now();
            std::hint::black_box(arm::merkle_root(&hashes));
            merkle.push(start.elapsed().as_secs_f64() * 1e6);
            let start = Instant::now();
            std::hint::black_box(case.block.view().auth_data_digests());
            auth.push(start.elapsed().as_secs_f64() * 1e6);
        }
        println!(
            "BREAKDOWN,{label},{},txids_us={:.0},merkle_us={:.0},auth_digests_us={:.0}",
            case.name,
            median(&mut ids),
            median(&mut merkle),
            median(&mut auth),
        );
    }

    for busy in [false, true] {
        let stop = Arc::new(AtomicBool::new(false));
        let contention = busy.then(|| start_rayon_contention(stop.clone()));
        std::thread::sleep(Duration::from_millis(50));
        for case in &cases {
            let _ = timed_phase(&rt, &network, &case.block);
            let mut lat = Vec::new();
            let mut gaps = Vec::new();
            let mut ticks = Vec::new();
            for _ in 0..samples {
                let (l, g, t) = timed_phase(&rt, &network, &case.block);
                lat.push(l);
                gaps.push(g);
                ticks.push(t as f64);
            }
            let (lo, hi) = (
                lat.iter().cloned().fold(f64::MAX, f64::min),
                lat.iter().cloned().fold(0.0, f64::max),
            );
            println!(
                "PHASE,{label},{},{},{:.0},{lo:.0},{hi:.0},{:.0},{:.0},{:.0}",
                if busy { "busy_rayon_synthetic" } else { "quiet" },
                case.name,
                median(&mut lat),
                median(&mut gaps),
                gaps.iter().cloned().fold(0.0, f64::max),
                median(&mut ticks),
            );
        }
        stop.store(true, Ordering::Relaxed);
        if let Some(handle) = contention {
            handle.join().expect("contention thread exits");
        }
    }
}

#[test]
#[ignore = "benchmark harness; run explicitly in release"]
fn run() {
    let label = std::env::var("BENCH_ARM").unwrap_or_else(|_| "unlabelled".into());
    println!(
        "CONFIG,{label},rayon_threads={},tokio_workers={},cpus={}",
        rayon::current_num_threads(),
        env_usize("BENCH_TOKIO_WORKERS", 1),
        std::thread::available_parallelism().map_or(0, |n| n.get())
    );
    match std::env::var("BENCH_MODE").as_deref() {
        Ok("merkle") => merkle_micro(&label),
        Ok("phase") => phase_bench(&label),
        _ => panic!("set BENCH_MODE=merkle or BENCH_MODE=phase"),
    }
}
