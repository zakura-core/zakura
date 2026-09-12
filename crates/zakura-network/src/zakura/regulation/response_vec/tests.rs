use super::*;
use crate::zakura::regulation::{ConnectionResponseMemory, ResponseMemory};

impl<T> ResponseVec<T> {
    pub(crate) fn reserve_for_test(
        &mut self,
        additional: usize,
        geometric: bool,
        memory: &ConnectionResponseMemory,
    ) -> Result<(), ResponseAdmissionError> {
        let plan = self.plan_capacity(additional, geometric)?;
        let funding = match &plan {
            Some(plan) => Some(
                memory
                    .try_reserve(plan.bytes())
                    .ok_or(ResponseAdmissionError::MemoryFull)?,
            ),
            None => None,
        };
        self.apply_capacity(plan, funding)
    }

    pub(crate) fn push_for_test(&mut self, value: T) {
        self.reserve_for_test(1, true, &ResponseMemory::default().connection())
            .expect("the fixture funds collection growth");
        self.push(value);
    }
}

#[test]
fn clearing_entries_does_not_refund_retained_capacity() {
    let node = ResponseMemory::default();
    let memory = node.connection();
    let baseline = node.reserved_for_test();
    let mut values = ResponseVec::<u64>::new();
    values.reserve_for_test(3, false, &memory).unwrap();
    for value in 0..3 {
        values.push(value);
    }
    assert_eq!(node.reserved_for_test(), baseline + 24);
    assert_eq!(values.remove(1), 1);
    assert_eq!(node.reserved_for_test(), baseline + 24);
    values.clear();
    assert!(values.is_empty());
    assert_eq!(values.capacity(), 3);
    assert_eq!(node.reserved_for_test(), baseline + 24);
    drop(values);
    assert_eq!(node.reserved_for_test(), baseline);
}

#[test]
fn growth_requires_old_and_new_storage_to_fit_together() {
    let setup =
        ResponseMemory::node_setup_bytes_for_test() + ResponseMemory::setup_bytes_for_test();
    let node = ResponseMemory::new(setup + 40, setup + 40);
    let memory = node.connection();
    let mut values = ResponseVec::<u64>::new();
    values.reserve_for_test(2, false, &memory).unwrap();
    values.push(10);
    values.push(20);
    let held = memory.try_reserve(8).unwrap();
    // A new 24-byte backing buffer fits alone, but cannot replace the live
    // 16-byte allocation while another owner retains eight bytes.
    assert_eq!(
        values.reserve_for_test(1, false, &memory),
        Err(ResponseAdmissionError::MemoryFull)
    );
    assert_eq!(&*values, &[10, 20]);
    assert_eq!(values.capacity(), 2);
    assert_eq!(node.reserved_for_test(), setup + 24);
    drop(held);
    values.reserve_for_test(1, false, &memory).unwrap();
    values.push(30);
    assert_eq!(&*values, &[10, 20, 30]);
    assert_eq!(node.reserved_for_test(), setup + 24);
    drop(values);
    assert_eq!(node.reserved_for_test(), setup);
}

#[test]
fn invalid_growth_plans_do_not_change_storage_or_charges() {
    let node = ResponseMemory::default();
    let memory = node.connection();
    let mut values = ResponseVec::<u64>::new();
    values.reserve_for_test(1, false, &memory).unwrap();
    values.push(9);
    let funded = node.reserved_for_test();
    assert!(matches!(
        values.plan_capacity(usize::MAX, false),
        Err(ResponseAdmissionError::MemoryFull)
    ));
    assert_eq!(&*values, &[9]);
    assert_eq!(node.reserved_for_test(), funded);
}

#[test]
fn zero_sized_entries_need_no_backing_allocation() {
    let mut values = ResponseVec::<()>::new();
    assert!(values.plan_capacity(3, true).unwrap().is_none());
    for _ in 0..3 {
        values.push(());
    }
    assert_eq!(values.len(), 3);
    assert!(values.funding.is_none());
}
