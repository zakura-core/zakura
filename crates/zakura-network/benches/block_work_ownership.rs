//! Measure native request admission and settlement without simulator capture.
#![allow(missing_docs)]

use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use zakura_network::zakura::{bench_work_request, BenchReceiptBatch, WorkRequestEnd};

fn request_lifetimes(c: &mut Criterion) {
    let mut group = c.benchmark_group("block_work_ownership");
    for count in [1, 16, 128] {
        group.throughput(Throughput::Elements(u64::from(count)));
        for end in [
            WorkRequestEnd::Unpublished,
            WorkRequestEnd::Queued,
            WorkRequestEnd::Partial,
            WorkRequestEnd::Received,
            WorkRequestEnd::Reset,
        ] {
            group.bench_with_input(
                BenchmarkId::new(format!("{end:?}"), count),
                &count,
                |b, &count| b.iter(|| bench_work_request(count, 4096, end)),
            );
        }
    }
    group.finish();
}

fn matched_receipts(c: &mut Criterion) {
    let mut group = c.benchmark_group("block_work_receipts");
    for count in [1, 16, 128] {
        group.throughput(Throughput::Elements(u64::from(count)));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &count| {
            b.iter_batched_ref(
                || BenchReceiptBatch::new(count, 4096),
                BenchReceiptBatch::receive_all,
                BatchSize::NumIterations(16),
            );
        });
    }
    group.finish();
}

criterion_group!(benches, request_lifetimes, matched_receipts);
criterion_main!(benches);
