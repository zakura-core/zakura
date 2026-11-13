//! Per-pass costs of the O(block bytes) encoding work a node does for one block.
//!
//! Builds full-size synthetic blocks from the real Sapling-era and NU5 mainnet
//! transactions in `zakura-test`, then times each pass a block can take between
//! the wire and RPC: decoding, encoding, sizing, hashing, and hex. Raw `memcpy`
//! and `sha256d` passes over the same bytes give a floor to compare against.
//!
//! Set `ZAKURA_BENCH_BLOCK_BYTES` to a comma-separated list of target sizes.
//! The default is `1000000,2000000`. Blocks above `MAX_BLOCK_BYTES` are decoded
//! as a header and a transaction list, which have no block-size limit, so the
//! per-pass costs of larger proposed limits can be measured directly.

// Disabled due to warnings in criterion macros
#![allow(missing_docs)]

use std::{
    hint::black_box,
    io::{Cursor, Write},
    sync::Arc,
};

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use zakura_chain::{
    block::{merkle, Block, Header, MAX_BLOCK_BYTES},
    parameters::NetworkUpgrade,
    serialization::{sha256d, ZcashDeserialize, ZcashSerialize},
    transaction::{Transaction, UnminedTx},
    transparent,
};

/// Non-coinbase Sapling-era and NU5 mainnet transactions, in vector order.
fn shielded_era_transactions() -> Vec<Arc<Transaction>> {
    zakura_test::vectors::MAINNET_BLOCKS
        .range(419_200..)
        .flat_map(|(_, bytes)| {
            Block::zcash_deserialize(Cursor::new(bytes))
                .expect("test vectors are valid blocks")
                .transactions
                .into_iter()
                .skip(1)
        })
        .collect()
}

/// A block of about `target_bytes` that cycles through `pool` after its coinbase.
fn synthetic_block(target_bytes: usize, pool: &[Arc<Transaction>]) -> Block {
    let mut block = Block::zcash_deserialize(Cursor::new(
        zakura_test::vectors::BLOCK_MAINNET_1687107_BYTES.as_slice(),
    ))
    .expect("test vectors are valid blocks");
    block.transactions.truncate(1);

    let mut size = block.zcash_serialized_size();
    for tx in pool.iter().cycle() {
        let tx_size = tx.zcash_serialized_size();
        if size + tx_size > target_bytes {
            break;
        }
        size += tx_size;
        block.transactions.push(tx.clone());
    }
    block
}

/// Decodes `bytes` as a block, without the consensus block-size limit.
fn decode_unbounded(mut bytes: &[u8]) -> Block {
    let header = Header::zcash_deserialize_from_slice(&mut bytes).expect("valid header");
    let transactions =
        Vec::<Arc<Transaction>>::zcash_deserialize_from_slice(&mut bytes).expect("valid list");
    assert!(bytes.is_empty(), "the block fills the bytes");
    Block {
        header: Arc::new(header),
        transactions,
    }
}

fn block_targets() -> Vec<usize> {
    std::env::var("ZAKURA_BENCH_BLOCK_BYTES")
        .unwrap_or_else(|_| "1000000,2000000".to_string())
        .split(',')
        .map(|size| size.trim().parse().expect("sizes are byte counts"))
        .collect()
}

fn block_encoding(c: &mut Criterion) {
    let pool = shielded_era_transactions();

    for target in block_targets() {
        let block = synthetic_block(target, &pool);
        let bytes = block.zcash_serialize_to_vec().expect("vec writes succeed");
        let hashes = block.transaction_hashes();
        let label = format!("{}KB/{}tx", bytes.len() / 1000, block.transactions.len());

        let mut group = c.benchmark_group("block_encoding");
        group.throughput(Throughput::Bytes(bytes.len() as u64));
        group.sample_size(20);

        group.bench_with_input(BenchmarkId::new("memcpy", &label), &bytes, |b, bytes| {
            b.iter(|| black_box(bytes.clone()))
        });
        group.bench_with_input(BenchmarkId::new("sha256d", &label), &bytes, |b, bytes| {
            b.iter(|| {
                let mut writer = sha256d::Writer::default();
                std::io::Write::write_all(&mut writer, bytes).expect("hash writes succeed");
                black_box(writer.finish())
            })
        });
        group.bench_with_input(
            BenchmarkId::new("hex_encode", &label),
            &bytes,
            |b, bytes| b.iter(|| black_box(hex::encode(bytes))),
        );
        if bytes.len() <= MAX_BLOCK_BYTES as usize {
            group.bench_with_input(
                BenchmarkId::new("deserialize_slice", &label),
                &bytes,
                |b, bytes| {
                    b.iter(|| {
                        let mut slice = bytes.as_slice();
                        black_box(Block::zcash_deserialize_from_slice(&mut slice).expect("valid"))
                    })
                },
            );
            group.bench_with_input(
                BenchmarkId::new("deserialize_cursor", &label),
                &bytes,
                |b, bytes| {
                    b.iter(|| {
                        black_box(Block::zcash_deserialize(Cursor::new(bytes)).expect("valid"))
                    })
                },
            );
        }
        group.bench_with_input(
            BenchmarkId::new("deserialize_unbounded", &label),
            &bytes,
            |b, bytes| b.iter(|| black_box(decode_unbounded(bytes))),
        );
        group.bench_with_input(BenchmarkId::new("serialize", &label), &block, |b, block| {
            b.iter(|| black_box(block.zcash_serialize_to_vec().expect("vec writes succeed")))
        });
        group.bench_with_input(
            BenchmarkId::new("serialized_size", &label),
            &block,
            |b, block| b.iter(|| black_box(block.zcash_serialized_size())),
        );
        group.bench_with_input(
            BenchmarkId::new("transaction_hashes", &label),
            &block,
            |b, block| b.iter(|| black_box(block.transaction_hashes())),
        );
        group.bench_with_input(
            BenchmarkId::new("txids_serial", &label),
            &block,
            |b, block| {
                b.iter(|| {
                    black_box(
                        block
                            .transactions
                            .iter()
                            .map(|tx| tx.hash())
                            .collect::<Vec<_>>(),
                    )
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("auth_data_digests", &label),
            &block,
            |b, block| b.iter(|| black_box(block.auth_data_digests())),
        );
        group.bench_with_input(
            BenchmarkId::new("merkle_root", &label),
            &hashes,
            |b, hashes| b.iter(|| black_box(hashes.iter().copied().collect::<merkle::Root>())),
        );
        group.bench_with_input(
            BenchmarkId::new("block_hash", &label),
            &block,
            |b, block| b.iter(|| black_box(block.hash())),
        );
        group.bench_with_input(
            BenchmarkId::new("unmined_tx_from_each", &label),
            &block,
            |b, block| {
                b.iter(|| {
                    black_box(
                        block
                            .transactions
                            .iter()
                            .map(UnminedTx::from)
                            .collect::<Vec<_>>(),
                    )
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("tx_serialize_each", &label),
            &block,
            |b, block| {
                b.iter(|| {
                    black_box(
                        block
                            .transactions
                            .iter()
                            .map(|tx| tx.zcash_serialize_to_vec().expect("vec writes succeed"))
                            .collect::<Vec<_>>(),
                    )
                })
            },
        );
        let spent_outputs: Vec<_> = block
            .transactions
            .iter()
            .skip(1)
            .map(|tx| {
                let nu = tx.network_upgrade().unwrap_or(NetworkUpgrade::Canopy);
                let outputs = Arc::new(vec![
                    transparent::Output {
                        value: 0.try_into().expect("zero is a valid amount"),
                        lock_script: transparent::Script::new(&[]),
                    };
                    tx.inputs().len()
                ]);
                tx.sighasher(nu, outputs.clone())
                    .expect("vector transactions convert to librustzcash");
                (tx.clone(), nu, outputs)
            })
            .collect();
        group.bench_with_input(
            BenchmarkId::new("sighasher_each", &label),
            &spent_outputs,
            |b, spent_outputs| {
                b.iter(|| {
                    for (tx, nu, outputs) in spent_outputs {
                        black_box(tx.sighasher(*nu, outputs.clone()).expect("converts"));
                    }
                })
            },
        );
        group.finish();
    }

    // Decode cost split by transaction version, to locate the expensive fields.
    let mut group = c.benchmark_group("block_decode_by_version");
    group.sample_size(20);
    for version in [4, 5] {
        let version_pool: Vec<_> = pool
            .iter()
            .filter(|tx| tx.version() == version)
            .cloned()
            .collect();
        let block = synthetic_block(2_000_000, &version_pool);
        let bytes = block.zcash_serialize_to_vec().expect("vec writes succeed");
        let label = format!(
            "v{version}/{}KB/{}tx",
            bytes.len() / 1000,
            block.transactions.len()
        );
        group.throughput(Throughput::Bytes(bytes.len() as u64));
        group.bench_with_input(
            BenchmarkId::new("deserialize_slice", &label),
            &bytes,
            |b, bytes| {
                b.iter(|| {
                    let mut slice = bytes.as_slice();
                    black_box(Block::zcash_deserialize_from_slice(&mut slice).expect("valid"))
                })
            },
        );
    }
    group.finish();
}

/// Compare the production writer with its previous backend, including small writes.
fn sha256d_backends(c: &mut Criterion) {
    use sha2::{Digest, Sha256};

    let mut group = c.benchmark_group("sha256d_backends");
    group.sample_size(20);
    for (length, chunk_size) in [
        (64usize, 32usize),
        (64, 64),
        (1487, 32),
        (1487, 1487),
        (50_000, 32),
        (50_000, 4096),
        (2_000_000, 4096),
    ] {
        let bytes: Vec<_> = (0..=255u8).cycle().take(length).collect();
        let label = format!("{length}bytes/{chunk_size}byte-writes");
        group.throughput(Throughput::Bytes(u64::try_from(length).unwrap()));
        group.bench_with_input(BenchmarkId::new("sha2", &label), &bytes, |b, bytes| {
            b.iter(|| {
                let mut hasher = Sha256::new();
                for chunk in black_box(bytes).chunks(chunk_size) {
                    hasher.update(chunk);
                }
                let digest: [u8; 32] = Sha256::digest(hasher.finalize()).into();
                black_box(digest)
            })
        });
        group.bench_with_input(BenchmarkId::new("ring", &label), &bytes, |b, bytes| {
            b.iter(|| {
                let mut writer = sha256d::Writer::default();
                for chunk in black_box(bytes).chunks(chunk_size) {
                    writer.write_all(chunk).unwrap();
                }
                black_box(writer.finish())
            })
        });
    }
    group.finish();
}

criterion_group!(benches, sha256d_backends, block_encoding);
criterion_main!(benches);
