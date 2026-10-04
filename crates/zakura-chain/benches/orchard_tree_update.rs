//! Benchmarks for one block's Orchard note commitment tree update.
//!
//! `update_orchard_note_commitment_tree` appends every Orchard note commitment
//! in a block and recalculates the root. The benchmark separates sequential
//! append, parallel batch append, and root recalculation.

#![allow(missing_docs)]

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use zakura_chain::orchard::tree::{NoteCommitmentTree, NoteCommitmentUpdate};

const PREFILL_LEAVES: u64 = 10_000;
const ACTION_COUNTS: [usize; 5] = [64, 128, 219, 436, 872];

fn commitments(start: u64, count: usize) -> Vec<NoteCommitmentUpdate> {
    (0..u64::try_from(count).expect("benchmark commitment count fits in u64"))
        .map(|index| NoteCommitmentUpdate::from(start + index))
        .collect()
}

fn prefilled_tree(prefix: u64) -> NoteCommitmentTree {
    let mut tree = NoteCommitmentTree::default();
    tree.append_batch(&commitments(
        1,
        usize::try_from(prefix).expect("benchmark prefill count fits in usize"),
    ))
    .expect("prefill fits in the tree");
    let _ = tree.root();
    tree
}

fn bench_tree_update(c: &mut Criterion) {
    let base = prefilled_tree(PREFILL_LEAVES);
    let mut group = c.benchmark_group("orchard_tree_update");

    for count in ACTION_COUNTS {
        let block = commitments(PREFILL_LEAVES + 1, count);
        group.throughput(Throughput::Elements(
            u64::try_from(count).expect("benchmark action count fits in u64"),
        ));

        group.bench_with_input(
            BenchmarkId::new("append_batch_and_root", count),
            &block,
            |b, block| {
                b.iter_batched(
                    || base.clone(),
                    |mut tree| {
                        tree.append_batch(black_box(block))
                            .expect("block fits in the tree");
                        black_box(tree.root())
                    },
                    criterion::BatchSize::SmallInput,
                )
            },
        );

        group.bench_with_input(
            BenchmarkId::new("append_sequential", count),
            &block,
            |b, block| {
                b.iter_batched(
                    || base.clone(),
                    |mut tree| {
                        for commitment in black_box(block) {
                            tree.append(*commitment).expect("block fits in the tree");
                        }
                        tree
                    },
                    criterion::BatchSize::SmallInput,
                )
            },
        );

        group.bench_with_input(
            BenchmarkId::new("append_batch_only", count),
            &block,
            |b, block| {
                b.iter_batched(
                    || base.clone(),
                    |mut tree| {
                        tree.append_batch(black_box(block))
                            .expect("block fits in the tree");
                        tree
                    },
                    criterion::BatchSize::SmallInput,
                )
            },
        );
    }

    group.bench_function("root_recalculate", |b| {
        b.iter_batched(
            || {
                let mut tree = base.clone();
                tree.append_batch(&commitments(PREFILL_LEAVES + 1, 1))
                    .expect("one leaf fits in the tree");
                tree
            },
            |tree| black_box(tree.root()),
            criterion::BatchSize::SmallInput,
        )
    });

    group.finish();
}

// The frontier retains the final leaf separately: an append of 2^k + 1
// leaves at prefix zero hashes one complete 2^k-leaf subtree.
const TRACKED_SUBTREE_LEAVES: u64 = 1u64 << zakura_chain::subtree::TRACKED_SUBTREE_HEIGHT;

const BOUNDARY_CASES: &[(u64, usize)] = &[
    (0, 1),
    (0, 2),
    (0, 7),
    (0, 8),
    (0, 9),
    (0, 31),
    (0, 32),
    (0, 33),
    (0, 63),
    (0, 64),
    (0, 65),
    (0, 127),
    (0, 128),
    (0, 129),
    (0, 255),
    (0, 256),
    (0, 257),
    (0, 383),
    (0, 384),
    (0, 385),
    (0, 436),
    (0, 872),
    (63, 129),
    (64, 129),
    (65, 129),
    (127, 129),
    (128, 129),
    (129, 129),
    (10_000, 64),
    (10_000, 65),
    (10_000, 128),
    (10_000, 129),
    (10_000, 383),
    (10_000, 384),
    (10_000, 385),
    (10_000, 436),
    (10_000, 872),
    (TRACKED_SUBTREE_LEAVES - 65, 65),
    (TRACKED_SUBTREE_LEAVES - 64, 64),
    (TRACKED_SUBTREE_LEAVES - 64, 65),
    (TRACKED_SUBTREE_LEAVES - 1, 1),
    (TRACKED_SUBTREE_LEAVES - 1, 2),
    (TRACKED_SUBTREE_LEAVES - 1, 65),
    (TRACKED_SUBTREE_LEAVES - 65, 436),
    (TRACKED_SUBTREE_LEAVES - 64, 436),
    (TRACKED_SUBTREE_LEAVES - 1, 436),
    (TRACKED_SUBTREE_LEAVES, 129),
];

fn bench_append_boundaries(c: &mut Criterion) {
    let mut group = c.benchmark_group("orchard_append_boundaries");
    group.sampling_mode(criterion::SamplingMode::Flat);
    let mut previous_prefix = None;
    let mut base = NoteCommitmentTree::default();

    for &(prefix, count) in BOUNDARY_CASES {
        if previous_prefix != Some(prefix) {
            base = prefilled_tree(prefix);
            previous_prefix = Some(prefix);
        }
        let block = commitments(prefix + 1, count);
        group.throughput(Throughput::Elements(
            u64::try_from(count).expect("benchmark action count fits in u64"),
        ));
        group.bench_with_input(
            BenchmarkId::new(format!("prefix_{prefix}"), count),
            &block,
            |b, block| {
                b.iter_batched(
                    || base.clone(),
                    |mut tree| {
                        tree.append_batch(black_box(block))
                            .expect("boundary benchmark fits in the tree");
                        black_box(tree.root())
                    },
                    criterion::BatchSize::SmallInput,
                )
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_tree_update, bench_append_boundaries);
criterion_main!(benches);
