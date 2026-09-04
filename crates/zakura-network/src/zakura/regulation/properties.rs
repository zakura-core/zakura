//! Message-independent ownership histories for the shared resource primitives.

use std::time::Duration;

use proptest::prelude::*;

use super::{test_support::RateModel, *};
use crate::zakura::testkit::TestClock;

enum RateOwner {
    Provisional(RateReservation<TestClock>),
    Committed(CommittedRateReservation<TestClock>),
}

#[derive(Clone, Copy, Debug)]
enum RateOperation {
    Reserve,
    Commit,
    Spend,
    Drop,
    Advance,
}

#[derive(Clone, Copy, Debug)]
enum ByteOperation {
    Reserve,
    Transfer,
    DropReservation,
    DropFrame,
}

proptest! {
    #[test]
    fn rate_histories_preserve_spending_and_clipped_refunds(
        capacity in 1u64..256,
        refill in 1u64..128,
        actions in prop::collection::vec((prop::sample::select(vec![
            RateOperation::Reserve, RateOperation::Commit, RateOperation::Spend,
            RateOperation::Drop, RateOperation::Advance,
        ]), 0usize..8, 0u16..512), 1..96),
    ) {
        let clock = TestClock::new();
        let budget = RateBudget::with_clock(capacity, refill, clock.clone()).unwrap();
        let mut model = RateModel::new(capacity, refill);
        let mut owners: [Option<RateOwner>; 8] = std::array::from_fn(|_| None);
        let mut refundable = [0u64; 8];

        for (operation, index, amount) in actions {
            let units = u64::from(amount);
            match operation {
                RateOperation::Reserve if owners[index].is_none() => {
                    let expected = model.reserve(units);
                    let actual = budget.try_reserve(units);
                    prop_assert_eq!(actual.is_ok(), expected);
                    if let Ok(owner) = actual {
                        owners[index] = Some(RateOwner::Provisional(owner));
                        refundable[index] = units;
                    }
                }
                RateOperation::Commit if matches!(owners[index], Some(RateOwner::Provisional(_))) => {
                    let Some(RateOwner::Provisional(owner)) = owners[index].take() else { unreachable!() };
                    let committed = owner.commit(units);
                    prop_assert_eq!(committed.is_ok(), units <= refundable[index]);
                    if let Ok(owner) = committed {
                        refundable[index] -= units;
                        owners[index] = Some(RateOwner::Committed(owner));
                    } else {
                        model.refund(refundable[index]);
                        refundable[index] = 0;
                    }
                }
                RateOperation::Spend => {
                    if let Some(RateOwner::Committed(owner)) = &mut owners[index] {
                        prop_assert_eq!(owner.spend(units).is_ok(), units <= refundable[index]);
                        if units <= refundable[index] { refundable[index] -= units; }
                    }
                }
                RateOperation::Drop => {
                    drop(owners[index].take());
                    model.refund(refundable[index]);
                    refundable[index] = 0;
                }
                RateOperation::Advance => {
                    // Non-integral refill intervals exercise fractional carry.
                    let elapsed = Duration::from_micros(units * 997);
                    clock.advance(elapsed);
                    model.advance(elapsed);
                }
                _ => {}
            }
            prop_assert_eq!(budget.available(), model.available());
        }
        drop(owners);
        for remaining in refundable { model.refund(remaining); }
        prop_assert_eq!(budget.available(), model.available());
    }

    #[test]
    fn byte_histories_conserve_reservations_and_frame_ownership(
        capacity in 1u64..128,
        actions in prop::collection::vec((prop::sample::select(vec![
            ByteOperation::Reserve, ByteOperation::Transfer,
            ByteOperation::DropReservation, ByteOperation::DropFrame,
        ]), 0usize..8, 0u64..160), 1..96),
    ) {
        let node = OutstandingByteBudget::new(capacity);
        let peer = OutstandingByteBudget::new(capacity);
        let mut reservations: [Option<(OutstandingByteReservation, OutstandingByteReservation)>; 8] = std::array::from_fn(|_| None);
        let mut frames: [Option<FrameLease>; 8] = std::array::from_fn(|_| None);
        let mut remainder = [0u64; 8];
        let mut frame_bytes = [0u64; 8];

        for (operation, index, bytes) in actions {
            let expected_used: u64 = remainder.iter().chain(&frame_bytes).sum();
            match operation {
                ByteOperation::Reserve if reservations[index].is_none() => {
                    let expected = bytes <= capacity - expected_used;
                    let actual = node.try_reserve(bytes).ok().flatten();
                    prop_assert_eq!(actual.is_some(), expected);
                    if let Some(node_owner) = actual {
                        let peer_owner = peer.try_reserve(bytes).unwrap().unwrap();
                        reservations[index] = Some((node_owner, peer_owner));
                        remainder[index] = bytes;
                    }
                }
                ByteOperation::Transfer if frames[index].is_none() => {
                    if let Some((node_owner, peer_owner)) = &mut reservations[index] {
                        let lease = OutstandingByteReservation::transfer_to_frame([node_owner, peer_owner], bytes);
                        prop_assert_eq!(lease.is_some(), bytes <= remainder[index]);
                        if let Some(lease) = lease {
                            remainder[index] -= bytes;
                            frame_bytes[index] = bytes;
                            frames[index] = Some(lease);
                        }
                    }
                }
                ByteOperation::DropReservation => {
                    drop(reservations[index].take());
                    remainder[index] = 0;
                }
                ByteOperation::DropFrame => {
                    drop(frames[index].take());
                    frame_bytes[index] = 0;
                }
                _ => {}
            }
            let expected: u64 = remainder.iter().chain(&frame_bytes).sum();
            prop_assert_eq!(node.reserved(), expected);
            prop_assert_eq!(peer.reserved(), expected);
        }
        drop(reservations);
        drop(frames);
        prop_assert_eq!(node.reserved(), 0);
        prop_assert_eq!(peer.reserved(), 0);
    }

    #[test]
    fn slot_histories_bound_owned_work(
        capacity in 1usize..8,
        actions in prop::collection::vec((any::<bool>(), 0usize..12), 1..96),
    ) {
        let budget = SlotBudget::new(capacity).unwrap();
        let mut permits: [Option<SlotPermit>; 12] = std::array::from_fn(|_| None);
        let mut occupied = [false; 12];
        for (acquire, index) in actions {
            if acquire && !occupied[index] {
                let expected = occupied.iter().filter(|owned| **owned).count() < capacity;
                permits[index] = budget.try_reserve();
                prop_assert_eq!(permits[index].is_some(), expected);
                occupied[index] = expected;
            } else if !acquire {
                drop(permits[index].take());
                occupied[index] = false;
            }
            prop_assert_eq!(budget.reserved(), occupied.iter().filter(|owned| **owned).count());
        }
        drop(permits);
        prop_assert_eq!(budget.reserved(), 0);
    }
}
