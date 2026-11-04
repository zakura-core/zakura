//! Research harness for complete V5/V6 transaction-ID SIMD batching.
//!
//! All field preparation, graph allocation and output collection are timed.
//! This example does not verify blocks or enable batching in the node.

// This research CLI writes CSV to stdout and correctness diagnostics to stderr.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use rayon::prelude::*;
use std::{
    hint::black_box,
    time::{Duration, Instant},
};
use zakura_chain::{
    block::Block,
    parameters::{NetworkUpgrade, TX_V6_VERSION_GROUP_ID},
    serialization::{SerializationError, ZcashDeserialize, ZcashSerialize},
    transaction::{probe_batch_txids, probe_oracle, Hash, Transaction},
};
// The shared fixtures also contain sighash inputs unused by this txid harness.
#[allow(dead_code)]
#[path = "../src/transaction/tests/ironwood_v6_tx_hash.rs"]
mod v6;
fn run(txs: &[&Transaction], mode: usize) -> Vec<Hash> {
    match mode {
        0 => txs.iter().map(|t| t.hash()).collect(),
        1 => {
            if txs.len() < 64 || rayon::current_num_threads() == 1 {
                run(txs, 0)
            } else {
                txs.par_iter().map(|t| t.hash()).collect()
            }
        }
        2 => probe_batch_txids(txs, false),
        3 => probe_batch_txids(txs, true),
        4 => txs
            .par_chunks(128)
            .flat_map_iter(|c| probe_batch_txids(c, true))
            .collect(),
        5 => txs
            .par_chunks(32)
            .flat_map_iter(|c| probe_batch_txids(c, true))
            .collect(),
        _ => unreachable!(),
    }
}
fn elapsed(txs: &[&Transaction], mode: usize, n: usize) -> f64 {
    let now = Instant::now();
    for _ in 0..n {
        black_box(run(black_box(txs), mode));
    }
    now.elapsed().as_nanos() as f64 / n as f64
}
fn main() {
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    eprintln!(
        "runtime many_degree={} avx2={} sse41={}",
        blake2b_simd::many::degree(),
        is_x86_feature_detected!("avx2"),
        is_x86_feature_detected!("sse4.1")
    );
    #[cfg(not(any(target_arch = "x86", target_arch = "x86_64")))]
    eprintln!("runtime many_degree={}", blake2b_simd::many::degree());
    let threads = std::env::var("THREADS")
        .unwrap_or("8".into())
        .parse()
        .unwrap();
    rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build_global()
        .unwrap();
    let mut official = Vec::new();
    let mut skipped = 0;
    for v in zakura_test::zip0244::TEST_VECTORS.iter() {
        match Transaction::zcash_deserialize(&v.tx[..]) {
            Ok(tx) => {
                assert_eq!(tx.hash(), Hash(v.txid));
                assert_eq!(probe_oracle(&tx), Hash(v.txid));
                official.push(tx)
            }
            Err(SerializationError::NonCanonicalShieldedProofSize) => skipped += 1,
            Err(e) => panic!("unexpected official parse error: {e}"),
        }
    }
    eprintln!(
        "official_v5 total={} accepted={} skipped_noncanonical={skipped}",
        zakura_test::zip0244::TEST_VECTORS.len(),
        official.len()
    );
    let mut v6s = Vec::new();
    for v in v6::TEST_VECTORS {
        let mut bytes = v.tx.to_vec();
        bytes[4..8].copy_from_slice(&TX_V6_VERSION_GROUP_ID.to_le_bytes());
        bytes[8..12]
            .copy_from_slice(&u32::from(NetworkUpgrade::Nu6_3.branch_id().unwrap()).to_le_bytes());
        let tx = Transaction::zcash_deserialize(&bytes[..]).unwrap();
        assert_eq!(tx.hash(), probe_oracle(&tx), "{}", v.scenario);
        v6s.push(tx);
    }
    eprintln!("v6 scenarios={}", v6s.len());
    let mut nu63: Vec<Transaction> = official
        .iter()
        .map(|tx| {
            let mut bytes = tx.zcash_serialize_to_vec().unwrap();
            bytes[8..12].copy_from_slice(
                &u32::from(NetworkUpgrade::Nu6_3.branch_id().unwrap()).to_le_bytes(),
            );
            let tx = Transaction::zcash_deserialize(&bytes[..]).unwrap();
            assert_eq!(tx.hash(), probe_oracle(&tx));
            tx
        })
        .collect();
    let mut transparent = Vec::new();
    let mut sapling = Vec::new();
    let mut orchard = Vec::new();
    for bytes in zakura_test::vectors::BLOCKS.iter() {
        let block = Block::zcash_deserialize(&bytes[..]).unwrap();
        for tx in block.transactions() {
            if tx.version() == 5 {
                let corpus = if tx.has_orchard_shielded_data() {
                    &mut orchard
                } else if tx.has_sapling_shielded_data() {
                    &mut sapling
                } else {
                    &mut transparent
                };
                if corpus.len() < 32 {
                    corpus.push(tx.clone());
                }
            }
        }
    }
    for tx in &orchard {
        let mut bytes = tx.zcash_serialize_to_vec().unwrap();
        bytes[8..12]
            .copy_from_slice(&u32::from(NetworkUpgrade::Nu6_3.branch_id().unwrap()).to_le_bytes());
        let tx = Transaction::zcash_deserialize(&bytes[..]).unwrap();
        assert_eq!(tx.hash(), probe_oracle(&tx));
        nu63.push(tx);
    }
    let ids: Vec<_> = v6s.iter().map(|tx| tx.hash()).collect();
    for (a, b) in [(2, 3), (4, 5), (6, 7)] {
        assert_eq!(ids[a], ids[b], "V6 anchor invariance");
    }
    for (a, b) in [(9, 10), (11, 12), (13, 14), (15, 16)] {
        assert_ne!(ids[a], ids[b], "V6 effect mutation");
    }
    let mut generated: Vec<Transaction> = transparent
        .iter()
        .map(|source| {
            let mut tx = v6s[0].clone();
            match (&mut tx, source) {
                (
                    Transaction::V6 {
                        inputs, outputs, ..
                    },
                    Transaction::V5 {
                        inputs: src_inputs,
                        outputs: src_outputs,
                        ..
                    },
                ) => {
                    *inputs = src_inputs.clone();
                    *outputs = src_outputs.clone();
                }
                _ => unreachable!(),
            }
            assert!(!tx.has_shielded_data());
            assert_eq!(tx.hash(), probe_oracle(&tx));
            let bytes = tx.zcash_serialize_to_vec().unwrap();
            assert_eq!(
                Transaction::zcash_deserialize(&bytes[..]).unwrap().hash(),
                tx.hash()
            );
            tx
        })
        .collect();
    generated.sort_by_key(|t| t.zcash_serialize_to_vec().unwrap().len());
    let compact_count = generated
        .iter()
        .take_while(|t| t.zcash_serialize_to_vec().unwrap().len() <= 240)
        .count();
    generated.truncate(if compact_count > 0 {
        compact_count
    } else {
        4.min(generated.len())
    });
    let compact_v5: Vec<Transaction> = transparent
        .iter()
        .filter(|tx| tx.zcash_serialize_to_vec().unwrap().len() <= 240)
        .cloned()
        .collect();
    assert!(!compact_v5.is_empty());
    for tx in &compact_v5 {
        assert_eq!(tx.hash(), probe_oracle(tx));
    }
    let mut mixed = official.clone();
    mixed.extend(v6s.clone());
    mixed.extend(nu63.clone());
    let corpora = [
        ("v5_official", official),
        ("v6", v6s),
        ("v5_nu63", nu63),
        ("v5_transparent", transparent),
        ("v5_sapling", sapling),
        ("v5_orchard", orchard),
        ("mixed", mixed),
        ("generated_v6_compact", generated),
        ("v5_compact", compact_v5),
    ];
    for (name, corpus) in &corpora {
        eprintln!(
            "corpus {name} count={} bytes={} sapling={} orchard={} ironwood={}",
            corpus.len(),
            corpus
                .iter()
                .map(|t| t.zcash_serialize_to_vec().unwrap().len())
                .sum::<usize>(),
            corpus
                .iter()
                .filter(|t| t.has_sapling_shielded_data())
                .count(),
            corpus
                .iter()
                .filter(|t| t.has_orchard_shielded_data())
                .count(),
            corpus
                .iter()
                .filter(|t| t.has_ironwood_shielded_data())
                .count()
        );
        for n in [0, 1, 3, 4, 5, 7, 8, 9, 15, 16, 17, 63, 64, 65, 256] {
            let txs: Vec<_> = (0..n).map(|i| &corpus[i % corpus.len()]).collect();
            let expected = run(&txs, 0);
            for mode in 1..6 {
                assert_eq!(run(&txs, mode), expected, "{name} count{n} mode{mode}");
            }
        }
    }
    eprintln!("correctness: all modes/lane boundaries/order passed");
    if std::env::var_os("CHECK_ONLY").is_some() {
        return;
    }
    let filter = std::env::var("CATEGORY").unwrap_or_default();
    let categories = std::env::var("CATEGORIES").unwrap_or(
        "v5_transparent,v5_sapling,v5_orchard,v6,mixed,generated_v6_compact,v5_compact".into(),
    );
    let modes: Vec<usize> = std::env::var("MODES")
        .unwrap_or("1,4,5".into())
        .split(',')
        .map(|s| s.parse().unwrap())
        .collect();
    assert!(modes.iter().all(|&m| m < 6));
    let counts: Vec<usize> = std::env::var("COUNTS")
        .unwrap_or("128,192,384,512,1024,2048,4096,8192,16384".into())
        .split(',')
        .map(|s| s.parse().unwrap())
        .collect();
    println!("category,count,bytes,size_class,threads,batch,round,mode,ns");
    for (name, corpus) in &corpora {
        if (!filter.is_empty() && filter != *name) || !categories.split(',').any(|s| s == *name) {
            continue;
        }
        for &count in &counts {
            let txs: Vec<_> = (0..count).map(|i| &corpus[i % corpus.len()]).collect();
            let bytes = txs
                .iter()
                .map(|t| t.zcash_serialize_to_vec().unwrap().len())
                .sum::<usize>();
            if bytes > 268_435_456 {
                eprintln!("skipped memory cap {name}/{count}/{bytes}");
                continue;
            }
            let size_class = if bytes > 2_000_000 {
                "synthetic_gt2mb"
            } else {
                "within_2mb"
            };
            let expected = run(&txs, 0);
            for &mode in &modes {
                assert_eq!(
                    run(&txs, mode),
                    expected,
                    "large differential/order {name}/{count}/{mode}"
                );
            }
            eprintln!("large correctness {name}/{count}/{bytes}/{size_class} passed");
            if let Ok(m) = std::env::var("PROFILE") {
                let mode = m.parse().unwrap();
                let start = Instant::now();
                while start.elapsed() < Duration::from_secs(4) {
                    black_box(run(&txs, mode));
                }
                continue;
            }
            let mut iterations = [1usize; 6];
            for &mode in &modes {
                loop {
                    let start = Instant::now();
                    elapsed(&txs, mode, iterations[mode]);
                    if start.elapsed() > Duration::from_millis(2) || iterations[mode] >= 4096 {
                        break;
                    }
                    iterations[mode] *= 2;
                }
            }
            for batch in 0..5 {
                for round in 0..7 {
                    for offset in 0..modes.len() {
                        let mode = modes[(offset + batch + round) % modes.len()];
                        let ns = elapsed(&txs, mode, iterations[mode]);
                        println!("{name},{count},{bytes},{size_class},{threads},{batch},{round},{mode},{ns:.2}");
                    }
                }
            }
        }
    }
}
