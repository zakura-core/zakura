//! Benchmarks for the note-commitment-tree Merkle hash (`combine`).
//!
//! `orchard_combine` exercises `MerkleCRH^Orchard` (Sinsemilla) through the
//! public `incrementalmerkletree::Hashable::combine`, the per-node hash that
//! dominates Orchard note-commitment tree updates during sync. `sapling_combine`
//! (Pedersen) is an unchanged control: it isolates Orchard-specific changes from
//! machine-to-machine variance.

// Disabled due to warnings in criterion macros
#![allow(missing_docs)]

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use incrementalmerkletree::{Hashable, Level};

use zakura_chain::orchard::tree::Node as OrchardNode;

/// Two distinct nodes built from small little-endian integers, which are
/// canonical field elements (as in the tree unit tests).
fn node_bytes(value: u64) -> [u8; 32] {
    let mut bytes = [0u8; 32];
    bytes[..8].copy_from_slice(&value.to_le_bytes());
    bytes
}

fn bench_combine(c: &mut Criterion) {
    // The layer prefix varies with the level but the per-node hash cost does
    // not, so a single representative level is enough.
    let level = Level::from(0);

    let orchard_a = OrchardNode::try_from(node_bytes(1))
        .expect("small little-endian integer is a canonical Pallas base");
    let orchard_b = OrchardNode::try_from(node_bytes(2))
        .expect("small little-endian integer is a canonical Pallas base");
    c.bench_function("orchard_combine", |b| {
        b.iter(|| OrchardNode::combine(level, black_box(&orchard_a), black_box(&orchard_b)))
    });

    let sapling_a =
        Option::<sapling_crypto::Node>::from(sapling_crypto::Node::from_bytes(node_bytes(1)))
            .expect("small little-endian integer is a canonical Jubjub base");
    let sapling_b =
        Option::<sapling_crypto::Node>::from(sapling_crypto::Node::from_bytes(node_bytes(2)))
            .expect("small little-endian integer is a canonical Jubjub base");
    c.bench_function("sapling_combine", |b| {
        b.iter(|| {
            sapling_crypto::Node::combine(level, black_box(&sapling_a), black_box(&sapling_b))
        })
    });
}

criterion_group!(benches, bench_combine);
criterion_main!(benches);
