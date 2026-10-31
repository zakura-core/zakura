//! Native request-lifetime workloads. These include queue construction and teardown.

use std::{num::NonZeroU64, sync::Arc};

use zakura_chain::block;
use zakura_rt::sync::CancellationToken;

use super::super::{request::BlockSizeEstimate, work_queue::WorkQueue};
use crate::zakura::transport::{ByteBudget, FrameWriteClaim};

/// The endpoint of a benchmark request's lifetime.
#[derive(Clone, Copy, Debug)]
pub enum WorkRequestEnd {
    /// Drop admission before publication.
    Unpublished,
    /// Drop publication before writer start.
    Queued,
    /// Receive half the bodies, then return the remaining request work.
    Partial,
    /// Receive every body after writer completion.
    Received,
    /// Reset all work while a started writer retains its claim.
    Reset,
}

/// Execute one request through production admission, publication and settlement.
///
/// This workload allocates queue metadata but no block payloads. It does not
/// measure decoding, QUIC, storage or asynchronous peer scheduling.
pub fn bench_work_request(count: u32, bytes: u32, end: WorkRequestEnd) {
    assert!(count > 0 && bytes >= 1024);
    let total = u64::from(count) * u64::from(bytes);
    let budget = ByteBudget::new(total);
    let work = Arc::new(WorkQueue::new(block::Height(0), budget.clone()));
    let authority = zakura_header_chain::BodyWorkAuthority {
        header: zakura_header_chain::HeaderWorkAuthority {
            header_generation: zakura_header_chain::HeaderGeneration::new(1),
            branch: zakura_header_chain::BranchId::new(block::Hash([0; 32]), block::Hash([1; 32])),
        },
        verified_generation: zakura_header_chain::VerifiedGeneration::new(1),
        body_work_epoch: zakura_header_chain::BodyWorkEpoch::default(),
    };
    assert_eq!(
        work.extend(
            authority,
            (1..=count).map(|height| (
                block::Height(height),
                block::Hash([2; 32]),
                BlockSizeEstimate::Advertised(bytes),
            )),
        ),
        usize::try_from(count).expect("request count fits usize")
    );
    let taken = work.take_request(
        block::Height(1),
        block::Height(count),
        usize::try_from(count).expect("request count fits usize"),
        total,
        1,
        NonZeroU64::new(1).expect("request ID is nonzero"),
    );
    let admitted = taken
        .admit(block::Height(0), CancellationToken::new())
        .expect("the empty budget admits the whole request");
    assert_eq!(budget.reserved(), total);
    let owner = admitted.owner();
    if matches!(end, WorkRequestEnd::Unpublished) {
        drop(admitted);
    } else {
        let claim = admitted.publish(|_| {}).expect("the request is current");
        if !matches!(end, WorkRequestEnd::Queued) {
            assert!(claim.try_start());
            match end {
                WorkRequestEnd::Received | WorkRequestEnd::Partial => {
                    claim.written();
                    let receipts = if matches!(end, WorkRequestEnd::Received) {
                        count
                    } else {
                        count / 2
                    };
                    for height in 1..=receipts {
                        let ended = work
                            .begin_receipt_for_owner(owner, block::Height(height))
                            .expect("receipt funding is coherent")
                            .expect("the request owns the height");
                        assert_eq!(ended.charge, u64::from(bytes));
                        let received = work
                            .claim_receipt_for_owner(owner, block::Height(height))
                            .expect("received funding is coherent");
                        drop((ended, received));
                    }
                    if matches!(end, WorkRequestEnd::Partial) {
                        work.release_reserved_and_return_items_detailed_for_owner(
                            owner,
                            ((receipts + 1)..=count).map(block::Height),
                        );
                    }
                }
                WorkRequestEnd::Reset => work.reset_above(block::Height(0)),
                WorkRequestEnd::Unpublished | WorkRequestEnd::Queued => unreachable!(),
            }
        }
        drop(claim);
    }
    assert_eq!(
        budget.reserved(),
        0,
        "owners settle before queue destruction"
    );
    assert_eq!(work.reserved_above(block::Height(0)), (0, 0));
    assert!(!budget.is_closed());
    std::hint::black_box(&work);
}
