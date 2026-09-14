//! An integration test links this crate without its unit-test allocator, which
//! is the case a caller hits when it forgets `#[global_allocator]`.

use zakura_test::allocations::measure;

#[test]
#[should_panic(expected = "install `TrackingAllocator`")]
fn measuring_without_the_tracking_allocator_fails_instead_of_reporting_zero() {
    let (_, measured) = measure(|| std::hint::black_box(vec![0u8; 4096]));
    // Unreachable: without the allocator hooks every field would read zero, so a
    // bound like this one would hold for an operation that allocated 4 KiB.
    assert_eq!(measured.requested_bytes, 0);
}
