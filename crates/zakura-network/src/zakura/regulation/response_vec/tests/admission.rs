//! One funded exchange admits multiple storage plans before any publication.

use super::*;
use crate::zakura::{
    regulation::{ExchangeOpenError, ResponseIndex, ResponseMatch, WriterFence},
    CloseCause,
};
use tokio_util::sync::CancellationToken;
use zakura_test::allocations::measure;

#[test]
fn exchange_vector_and_index_growth_share_one_admission() {
    let node = ResponseMemory::default();
    let memory = node.connection();
    let fence =
        WriterFence::with_memory(CancellationToken::new(), CloseCause::new(), memory.clone());
    let baseline = memory.reserved_for_test();
    let mut values = ResponseVec::<u64>::new();
    let mut index = ResponseIndex::<u64>::new();
    let vector_plan = values.plan_capacity(2, false).unwrap();
    let index_plan = index.plan_capacity(2, false).unwrap();
    let bytes = vector_plan.as_ref().unwrap().bytes() + index_plan.as_ref().unwrap().bytes();
    let full = memory.try_reserve(16 * 1024 * 1024 - baseline).unwrap();
    let (denied, allocations) = measure(|| fence.try_open_with_retained_memory(24, bytes));
    assert_eq!(denied.unwrap_err(), ExchangeOpenError::MemoryFull);
    assert_eq!(allocations.requested_bytes, 0);
    assert!(values.is_empty());
    assert_eq!(values.capacity(), 0);
    assert_eq!(index.find(7), ResponseMatch::Missing);
    drop(full);

    let (mut exchange, mut funding) = fence.try_open_with_retained_memory(24, bytes).unwrap();
    let writer = exchange.writer();
    assert_eq!(
        memory.reserved_for_test(),
        baseline + WriterFence::admission_bytes(24, bytes).unwrap()
    );
    values
        .apply_capacity_from(vector_plan, &mut funding)
        .unwrap();
    index.apply_capacity_from(index_plan, &mut funding);
    assert_eq!(funding.take().unwrap().bytes(), 0);
    assert!(writer.publish(|| {
        values.push(7);
        index.insert(7, 0);
    }));
    assert!(writer.try_start(|| true));
    exchange.end();
    drop((exchange, writer));
    assert_eq!(memory.reserved_for_test(), baseline + bytes);
    assert_eq!(index.find(7), ResponseMatch::Unique(0));
    // Empty logical storage still owns backing allocations.
    index.remove(7, 0);
    values.clear();
    assert_eq!(memory.reserved_for_test(), baseline + bytes);
    drop((index, values));
    assert_eq!(memory.reserved_for_test(), baseline);
}
