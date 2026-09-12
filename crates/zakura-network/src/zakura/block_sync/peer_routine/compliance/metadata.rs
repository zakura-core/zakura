//! Compare the request allocation plan with retained storage at count boundaries.

use super::*;
use crate::zakura::{
    regulation::{ResponseMemory, ResponseScope},
    transport::FrameWriteClaim,
    CloseCause,
};
use zakura_test::allocations::measure;

proptest! {
    #[test]
    fn r02_generated_request_metadata_plan_funds_retained_allocations(count in 1usize..=128) {
        check_request_metadata(count)?;
    }
}

#[test]
fn r02_request_metadata_capacity_boundaries() {
    for count in [1, 2, 3, 4, 7, 8, 9, 127, 128] {
        check_request_metadata(count).unwrap();
    }
}

fn check_request_metadata(count: usize) -> Result<(), proptest::test_runner::TestCaseError> {
    let conversion_error = |error: std::num::TryFromIntError| {
        proptest::test_runner::TestCaseError::fail(error.to_string())
    };
    let height_count = u32::try_from(count).map_err(conversion_error)?;
    let body_count = u64::from(height_count);
    let node = ResponseMemory::default();
    let memory = node.connection();
    let scope =
        ResponseScope::with_memory(CancellationToken::new(), CloseCause::new(), memory.clone());
    let planned = RequestWrite::metadata_bytes(count).unwrap()
        + u64::try_from(count * std::mem::size_of::<ExpectedBlock>()).map_err(conversion_error)?;
    let mut authorization = scope.authorize_with_metadata(planned).map_err(|error| {
        proptest::test_runner::TestCaseError::fail(format!("request plan admission: {error:?}"))
    })?;
    let funded = memory.reserved_for_test();
    let work = Arc::new(WorkQueue::new(block::Height(0)));
    work.set_estimate_floor_for_tests(1);
    work.extend(
        super::super::super::test_work_scope(),
        (1..=count).map(|height| {
            (
                block::Height(u32::try_from(height).unwrap()),
                block::Hash([u8::try_from(height).unwrap(); 32]),
                BlockSizeEstimate::Confirmed(1),
            )
        }),
    );
    let items = work.take_for_request(
        block::Height(1),
        block::Height(height_count),
        count,
        u64::MAX,
        1,
        NonZeroU64::new(1).unwrap(),
    );
    prop_assert_eq!(items.len(), count);
    prop_assert!(items.capacity() <= count);
    let item_bytes =
        u64::try_from(items.capacity() * std::mem::size_of::<(block::Height, WorkItem)>())
            .map_err(conversion_error)?;
    let mut bodies = ByteBudget::new(body_count);
    prop_assert!(bodies.try_reserve(body_count));
    let owner = items[0].1.owner.unwrap();
    let cancel = CancellationToken::new();
    let ((write, expected), allocations) = measure(|| {
        let expected: Vec<ExpectedBlock> = items
            .iter()
            .map(|(height, item)| ExpectedBlock {
                height: *height,
                hash: item.hash,
                estimated_bytes: item.estimated_bytes,
            })
            .collect();
        let write = RequestWrite::new(
            owner,
            items,
            work.clone(),
            bodies.clone(),
            cancel,
            authorization.write_permission(),
        );
        (write, expected)
    });
    prop_assert!(allocations.retained_bytes > 0);
    let peak_live_bytes = u64::try_from(allocations.peak_live_bytes).map_err(conversion_error)?;
    prop_assert!(
        item_bytes + peak_live_bytes <= planned,
        "{} item bytes + {} allocation bytes exceed plan {}",
        item_bytes,
        allocations.peak_live_bytes,
        planned,
    );
    let status = write.status();
    prop_assert!(write.publish(|| {}), "the prepared request can publish");
    prop_assert!(write.try_start());
    write.written();
    authorization.finish();
    drop((write, expected, authorization));
    prop_assert_eq!(memory.reserved_for_test(), funded);
    prop_assert!(!status.was_skipped());
    drop(status);
    prop_assert_eq!(
        memory.reserved_for_test(),
        ResponseMemory::setup_bytes_for_test() + ResponseScope::setup_bytes_for_test()
    );
    drop(scope);
    prop_assert_eq!(
        memory.reserved_for_test(),
        ResponseMemory::setup_bytes_for_test()
    );
    drop(memory);
    prop_assert_eq!(
        node.reserved_for_test(),
        ResponseMemory::node_setup_bytes_for_test()
    );
    Ok(())
}
