//! Measure native request admission and settlement without simulator capture.
#![allow(missing_docs)]

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use zakura_network::zakura::{bench_work_request, WorkRequestEnd};

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

criterion_group!(benches, request_lifetimes);
criterion_main!(benches);
