//! Benchmarks for per-version transaction deserialization and serialization.
//!
//! Zcash has six transaction versions with increasingly complex structures:
//! - V1: transparent only (pre-Overwinter)
//! - V2: adds Sprout JoinSplits with BCTV14 proofs
//! - V3: Overwinter (adds expiry_height, version group ID)
//! - V4: Sapling (adds shielded spends/outputs with Groth16 proofs, non-sequential field order)
//! - V5: NU5 (adds Orchard actions with Halo2 proofs, different field order than V4)
//! - V6: NU6.3 (adds Ironwood actions)
//!
//! V4 deserialization is notably more complex than earlier versions because the
//! binding signature is at the end of the transaction, requiring non-sequential
//! parsing. V5 introduces yet another field ordering and Orchard support. V6
//! extends that format with Ironwood support.
//!
//! # Test data
//!
//! Transactions are extracted from real mainnet blocks in `zakura-test` vectors.
//! Versions V1 through V5 are represented by transactions from blocks at the
//! appropriate network upgrade heights. The benchmark serializes each transaction
//! to bytes first, then benchmarks both deserialization and serialization. The
//! ZIP-244 digest benchmarks also construct an Ironwood-only V6 transaction from
//! a real shielded bundle.

// Disabled due to warnings in criterion macros
#![allow(missing_docs)]

use std::{io::Cursor, sync::Arc};

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};

use zakura_chain::{
    block::{Block, Height},
    parameters::NetworkUpgrade,
    serialization::{ZcashDeserialize, ZcashSerialize},
    transaction::{LockTime, Transaction},
};

/// Extracts the first transaction matching a given version from a block.
fn first_tx_of_version(block: &Block, version: u32) -> Option<Vec<u8>> {
    block
        .transactions
        .iter()
        .find(|tx| tx.version() == version)
        .map(|tx| tx.zcash_serialize_to_vec().expect("valid transaction"))
}

/// Extracts the first Orchard-only v5 transaction from a block.
fn first_v5_orchard_only_tx(block: &Block) -> Option<Arc<Transaction>> {
    block
        .transactions
        .iter()
        .find(|tx| {
            tx.version() == 5
                && !tx.has_transparent_inputs_or_outputs()
                && !tx.has_sapling_shielded_data()
                && tx.has_orchard_shielded_data()
        })
        .cloned()
}

/// Constructs an Ironwood-only v6 transaction from a real Orchard bundle.
fn v6_ironwood_only_tx(orchard_tx: &Transaction) -> Arc<Transaction> {
    let ironwood_shielded_data = orchard_tx
        .orchard_shielded_data()
        .cloned()
        .expect("Orchard-only tx has Orchard shielded data");

    Arc::new(Transaction::V6 {
        network_upgrade: NetworkUpgrade::Nu6_3,
        lock_time: LockTime::unlocked(),
        expiry_height: Height(1),
        inputs: Vec::new(),
        outputs: Vec::new(),
        sapling_shielded_data: None,
        orchard_shielded_data: None,
        ironwood_shielded_data: Some(ironwood_shielded_data),
    })
}

fn bench_transaction_deserialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("Transaction Deserialization");

    // Collect (label, serialized_tx_bytes) pairs for each version.
    let mut tx_samples: Vec<(&str, Vec<u8>)> = Vec::new();

    // V1 — transparent coinbase from genesis-era block.
    let block = Block::zcash_deserialize(Cursor::new(
        zakura_test::vectors::BLOCK_MAINNET_1_BYTES.as_slice(),
    ))
    .expect("valid block");
    if let Some(bytes) = first_tx_of_version(&block, 1) {
        tx_samples.push(("V1 transparent", bytes));
    }

    // V2 — first block with a Sprout JoinSplit (BCTV14 proofs).
    let block = Block::zcash_deserialize(Cursor::new(
        zakura_test::vectors::BLOCK_MAINNET_396_BYTES.as_slice(),
    ))
    .expect("valid block");
    if let Some(bytes) = first_tx_of_version(&block, 2) {
        tx_samples.push(("V2 sprout joinsplit", bytes));
    }

    // V3 — first Overwinter block.
    let block = Block::zcash_deserialize(Cursor::new(
        zakura_test::vectors::BLOCK_MAINNET_347500_BYTES.as_slice(),
    ))
    .expect("valid block");
    if let Some(bytes) = first_tx_of_version(&block, 3) {
        tx_samples.push(("V3 overwinter", bytes));
    }

    // V4 — Sapling block with shielded data.
    let block = Block::zcash_deserialize(Cursor::new(
        zakura_test::vectors::BLOCK_MAINNET_419201_BYTES.as_slice(),
    ))
    .expect("valid block");
    if let Some(bytes) = first_tx_of_version(&block, 4) {
        tx_samples.push(("V4 sapling", bytes));
    }

    // V5 — NU5 block with Orchard data.
    let block = Block::zcash_deserialize(Cursor::new(
        zakura_test::vectors::BLOCK_MAINNET_1687107_BYTES.as_slice(),
    ))
    .expect("valid block");
    if let Some(bytes) = first_tx_of_version(&block, 5) {
        tx_samples.push(("V5 orchard", bytes));
    }

    for (label, tx_bytes) in &tx_samples {
        group.bench_with_input(
            BenchmarkId::new("deserialize", label),
            tx_bytes,
            |b, bytes| b.iter(|| Transaction::zcash_deserialize(Cursor::new(bytes)).unwrap()),
        );
    }

    group.finish();

    let mut group = c.benchmark_group("Transaction Serialization");

    for (label, tx_bytes) in &tx_samples {
        let tx = Transaction::zcash_deserialize(Cursor::new(tx_bytes)).unwrap();

        group.bench_with_input(BenchmarkId::new("serialize", label), &tx, |b, tx| {
            b.iter(|| tx.zcash_serialize_to_vec().unwrap())
        });
    }

    group.finish();
}

fn bench_zip244_digests(c: &mut Criterion) {
    let nu5_blocks = [
        zakura_test::vectors::BLOCK_MAINNET_1687107_BYTES.as_slice(),
        zakura_test::vectors::BLOCK_MAINNET_1687108_BYTES.as_slice(),
        zakura_test::vectors::BLOCK_MAINNET_1687113_BYTES.as_slice(),
        zakura_test::vectors::BLOCK_MAINNET_1687118_BYTES.as_slice(),
        zakura_test::vectors::BLOCK_MAINNET_1687121_BYTES.as_slice(),
    ];
    let orchard_tx = nu5_blocks
        .into_iter()
        .find_map(|block_bytes| {
            let block = Block::zcash_deserialize(Cursor::new(block_bytes)).expect("valid block");
            first_v5_orchard_only_tx(&block)
        })
        .expect("vectors contain an Orchard-only v5 tx");
    let ironwood_tx = v6_ironwood_only_tx(&orchard_tx);

    assert_eq!(ironwood_tx.version(), 6);
    assert!(!ironwood_tx.has_transparent_inputs_or_outputs());
    assert!(!ironwood_tx.has_sapling_shielded_data());
    assert!(!ironwood_tx.has_orchard_shielded_data());
    assert!(ironwood_tx.has_ironwood_shielded_data());

    let mut group = c.benchmark_group("ZIP-244 Digests");
    group.noise_threshold(0.01);
    group.sample_size(1000);

    for (label, tx) in [("orchard_only", orchard_tx), ("ironwood_only", ironwood_tx)] {
        group.bench_function(format!("auth_digest/{label}"), |b| {
            b.iter(|| {
                black_box(&tx)
                    .auth_digest()
                    .expect("v5 and v6 txs have auth digests")
            })
        });

        group.bench_function(format!("txid_and_auth_digest/{label}"), |b| {
            b.iter(|| black_box(&tx).txid_and_auth_digest())
        });
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().noise_threshold(0.1).sample_size(50);
    targets = bench_transaction_deserialize, bench_zip244_digests
}
criterion_main!(benches);
