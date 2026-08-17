#![allow(missing_docs)]
#![allow(unsafe_code)]

use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::atomic::{AtomicUsize, Ordering},
};

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use zakura_header_chain::RetentionBenchmarkFixture;

struct CountingAllocator;

static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

// SAFETY: This wrapper delegates every allocation operation to `System` without
// changing its pointer or layout contract.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        // SAFETY: The caller provides the layout required by `GlobalAlloc`.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: The pointer and layout came from the delegated allocator.
        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        // SAFETY: The pointer and layout came from the delegated allocator.
        unsafe { System.realloc(pointer, layout, size) }
    }
}

#[global_allocator]
static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator;

fn measured_allocations<T>(operation: impl FnOnce() -> T) -> (T, usize) {
    ALLOCATIONS.store(0, Ordering::Relaxed);
    let result = operation();
    let allocations = ALLOCATIONS.swap(0, Ordering::Relaxed);
    (result, allocations)
}

fn retention_allocation_limit(group: &str, percent: usize) -> usize {
    let baseline: toml::Value = toml::from_str(include_str!("retention-allocation-baseline.toml"))
        .expect("the checked-in retention allocation baseline is valid TOML");
    baseline[group][format!("percent_{percent}")]["allocations"]
        .as_integer()
        .and_then(|value| usize::try_from(value).ok())
        .expect("the PR 586 allocation limit is a nonnegative integer")
}

fn retention(c: &mut Criterion) {
    let mut ordinary = c.benchmark_group("header_chain_retention_ordinary");
    ordinary.sample_size(20);
    for percent in [25, 50, 90, 100] {
        let mut fixture = RetentionBenchmarkFixture::at_v1_limit_percent(percent)
            .expect("the benchmark graph is coherent");
        let (structural, allocations) = measured_allocations(|| fixture.ordinary_check());
        let structural = structural.expect("the ordinary exact-limit check succeeds");
        assert!(allocations <= retention_allocation_limit("ordinary", percent));
        assert_eq!(
            allocations, 0,
            "ordinary admission must remain allocation-free"
        );
        assert!(!structural.admission_refused);
        assert_eq!(structural.protected_path_visits, 0);
        assert_eq!(structural.candidate_nodes_scanned, 0);
        assert_eq!(structural.evicted_nodes, 0);
        assert_eq!(structural.graph_workspaces, 0);
        ordinary.bench_with_input(BenchmarkId::from_parameter(percent), &percent, |b, _| {
            b.iter(|| black_box(fixture.ordinary_check().expect("retention succeeds")));
        });
    }
    ordinary.finish();

    let mut refusal = c.benchmark_group("header_chain_retention_protected_refusal");
    refusal.sample_size(10);
    for percent in [25, 50, 90, 100] {
        let mut fixture = RetentionBenchmarkFixture::at_v1_limit_percent(percent)
            .expect("the benchmark graph is coherent");
        let (structural, allocations) = measured_allocations(|| fixture.protected_refusal());
        let structural = structural.expect("the protected refusal check succeeds");
        assert!(
            allocations <= retention_allocation_limit("protected_refusal", percent),
            "protected refusal allocations exceeded the retention baseline"
        );
        assert!(structural.admission_refused);
        assert_eq!(
            structural.protected_path_visits,
            zakura_header_chain::MAX_NON_FINALIZED_NODES_V1 * percent / 100 + 1
        );
        assert_eq!(structural.candidate_nodes_scanned, 0);
        assert_eq!(structural.evicted_nodes, 0);
        assert_eq!(structural.graph_workspaces, 1);
        refusal.bench_with_input(BenchmarkId::from_parameter(percent), &percent, |b, _| {
            b.iter(|| black_box(fixture.protected_refusal().expect("retention succeeds")));
        });
    }
    refusal.finish();
}

criterion_group!(benches, retention);
criterion_main!(benches);
