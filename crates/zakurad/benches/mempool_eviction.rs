//! Full-pool mempool selection and admission benchmarks.

// Criterion generates an undocumented public harness function.
#![allow(missing_docs)]

use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion};
use zakurad::components::mempool::mempool_eviction_benchmarks;

fn benchmark(c: &mut Criterion) {
    mempool_eviction_benchmarks(|name, sample| {
        c.bench_function(name, |b| {
            b.iter_custom(|iterations| (0..iterations).map(|_| sample()).sum())
        });
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(20)
        .warm_up_time(Duration::from_millis(50))
        .measurement_time(Duration::from_millis(200));
    targets = benchmark
}
criterion_main!(benches);
