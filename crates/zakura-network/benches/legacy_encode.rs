//! Legacy-protocol message encoding: the cost of framing a block or transaction for
//! one peer, as the node does each time it serves one.
//!
//! Blocks are built from the real Sapling-era and NU5 mainnet transactions in
//! `zakura-test`, at the sizes in `ZAKURA_BENCH_BLOCK_BYTES` (comma-separated,
//! default `1000000,2000000`). Each block is encoded repeatedly by one codec, so after
//! the first send its checksum comes from the codec's shared block-checksum cache,
//! as it does when a node serves the same block to many peers.
#![allow(missing_docs)]

use std::{hint::black_box, io::Cursor, sync::Arc};

use bytes::BytesMut;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use tokio_util::codec::Encoder;

use zakura_chain::{
    block::Block,
    serialization::{ZcashDeserialize, ZcashDeserializeInto, ZcashSerialize},
    transaction::Transaction,
};
use zakura_network::{Codec, Message};

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

fn block_targets() -> Vec<usize> {
    std::env::var("ZAKURA_BENCH_BLOCK_BYTES")
        .unwrap_or_else(|_| "1000000,2000000".to_string())
        .split(',')
        .map(|size| size.trim().parse().expect("sizes are byte counts"))
        .collect()
}

fn bench_encode(c: &mut Criterion) {
    let pool = shielded_era_transactions();
    let mut cases: Vec<(String, Message)> = block_targets()
        .into_iter()
        .map(|target| {
            let block = synthetic_block(target, &pool);
            let label = format!(
                "block_{}KB_{}tx",
                block.zcash_serialized_size() / 1000,
                block.transactions.len()
            );
            (label, Message::Block(Arc::new(block).into()))
        })
        .collect();
    cases.push((
        "tx_small".to_string(),
        Message::Tx(
            zakura_test::vectors::DUMMY_TX1
                .zcash_deserialize_into::<Transaction>()
                .expect("test vector is a valid transaction")
                .into(),
        ),
    ));

    let mut group = c.benchmark_group("legacy_encode");
    group.sample_size(30);
    for (label, message) in cases {
        // A real connection reuses its write buffer, clearing it once the frame is sent.
        let mut codec = Codec::builder().finish();
        codec.reconfigure_full_body_len();
        let mut dst = BytesMut::new();
        codec
            .encode(message.clone(), &mut dst)
            .expect("message fits in a protocol message");
        group.throughput(Throughput::Bytes(dst.len() as u64));

        group.bench_with_input(BenchmarkId::new("encode", label), &message, |b, message| {
            b.iter(|| {
                dst.clear();
                codec
                    .encode(black_box(message.clone()), &mut dst)
                    .expect("message fits in a protocol message");
                black_box(&dst);
            })
        });
    }
    group.finish();
}

criterion_group!(benches, bench_encode);
criterion_main!(benches);
