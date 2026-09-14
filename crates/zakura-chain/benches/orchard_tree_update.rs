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

fn prefilled_tree() -> NoteCommitmentTree {
    let mut tree = NoteCommitmentTree::default();
    tree.append_batch(&commitments(
        1,
        usize::try_from(PREFILL_LEAVES).expect("benchmark prefill count fits in usize"),
    ))
    .expect("prefill fits in the tree");
    let _ = tree.root();
    tree
}

fn bench_tree_update(c: &mut Criterion) {
    let base = prefilled_tree();
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

criterion_group!(benches, bench_tree_update);
criterion_main!(benches);
