//! Chooses the smallest `block` message body whose checksum is shared between connections.
//!
//! A send whose checksum is shared looks it up using the hash carried by its message.
//! Otherwise the send hashes its whole body. With `n` sends of one block, sharing costs `n` lookups and one body
//! hash, against `n` body hashes, so it saves time once a body hash costs more than
//! `n / (n - 1)` lookups. Two sends is the worst case: twice a lookup.
//!
//! `body_hash` times the hash of each body size, and `cached_lookup` times one lookup in a
//! full cache. Compare the two to find the size where a body hash costs twice a lookup.
#![allow(missing_docs)]

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use zakura_chain::{block::Block, serialization::ZcashDeserialize};

use zakura_network::checksum_bench::{hash_body, CachedBlock, MIN_SHARED_BODY_LEN};

/// Body sizes around the threshold, in KiB.
const BODY_KIB: [usize; 10] = [1, 2, 4, 8, 16, 32, 64, 128, 256, 1024];

fn body_hash(c: &mut Criterion) {
    let mut group = c.benchmark_group("body_hash");
    for kib in BODY_KIB {
        // Hashing time does not depend on the bytes, only on their length.
        // `i % 251` is below 256, so it fits in a byte.
        let body: Vec<u8> = (0..kib * 1024).map(|i| (i % 251) as u8).collect();
        // Widening usize to u64 is lossless on supported targets.
        group.throughput(Throughput::Bytes(body.len() as u64));
        group.bench_with_input(BenchmarkId::from_parameter(kib), &body, |b, body| {
            b.iter(|| hash_body(black_box(body)))
        });
    }
    group.finish();
}

fn cached_lookup(c: &mut Criterion) {
    let block = Block::zcash_deserialize(&zakura_test::vectors::BLOCK_MAINNET_419199_BYTES[..])
        .expect("the block test vector deserializes");
    let cached = CachedBlock::new(&block, MIN_SHARED_BODY_LEN);
    let hash = block.hash();
    c.bench_function("cached_lookup", |b| {
        b.iter(|| cached.lookup(black_box(hash)))
    });
}

criterion_group!(benches, body_hash, cached_lookup);
criterion_main!(benches);
