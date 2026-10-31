//! Native request-lifetime workloads. These include queue construction and teardown.

use std::{num::NonZeroU64, sync::Arc};

use zakura_chain::block;
use zakura_rt::sync::CancellationToken;

use super::super::{
    request::BlockSizeEstimate,
    work_queue::{AdmitRefusal, WorkQueue},
};
use crate::zakura::transport::{ByteBudget, ByteReservation, FrameWriteClaim};

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
    let (work, budget) = pending_work(count, bytes, u64::from(count) * u64::from(bytes));
    let total = u64::from(count) * u64::from(bytes);
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
                        drop(ended);
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

fn pending_work(count: u32, bytes: u32, limit: u64) -> (Arc<WorkQueue>, ByteBudget) {
    assert!(count > 0 && bytes >= 1024);
    let budget = ByteBudget::new(limit);
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
    (work, budget)
}

/// A written request prepared outside the timed receipt interval.
pub struct BenchReceiptBatch {
    work: Arc<WorkQueue>,
    budget: ByteBudget,
    owner: zakura_header_chain::BodyWorkOwner,
    count: u32,
    bytes: u32,
}

impl BenchReceiptBatch {
    /// Admit, publish and complete the writer before measuring receipts.
    pub fn new(count: u32, bytes: u32) -> Self {
        let (work, budget) = pending_work(count, bytes, u64::from(count) * u64::from(bytes));
        let total = u64::from(count) * u64::from(bytes);
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
        let owner = admitted.owner();
        let claim = admitted.publish(|_| {}).expect("the request is current");
        assert!(claim.try_start());
        claim.written();
        drop(claim);
        Self {
            work,
            budget,
            owner,
            count,
            bytes,
        }
    }

    /// Settle every matched receipt once. Queue destruction follows the timed interval.
    pub fn receive_all(&mut self) {
        assert_eq!(
            self.budget.reserved(),
            u64::from(self.count) * u64::from(self.bytes)
        );
        for raw in 1..=self.count {
            let ended = self
                .work
                .begin_receipt_for_owner(self.owner, block::Height(raw))
                .expect("receipt funding is coherent")
                .expect("the request owns the height");
            assert_eq!(ended.charge, u64::from(self.bytes));
            drop(ended);
        }
        assert_eq!(self.budget.reserved(), 0);
    }
}

/// Funded receipts prepared without queue metadata or locking.
pub struct BenchBudgetReceipts {
    budget: ByteBudget,
    funding: ByteReservation,
    count: u32,
    bytes: u32,
}

impl BenchBudgetReceipts {
    /// Acquire the same aggregate funding that a published queue owns.
    pub fn new(count: u32, bytes: u32) -> Self {
        assert!(count > 0 && bytes > 0);
        let total = u64::from(count) * u64::from(bytes);
        let budget = ByteBudget::new(total);
        let funding = budget
            .reserve_owned(total)
            .expect("the budget starts empty");
        Self {
            budget,
            funding,
            count,
            bytes,
        }
    }

    /// Transfer and release each receipt without entering a queue transition.
    pub fn release_all(&mut self) {
        assert_eq!(
            self.budget.reserved(),
            u64::from(self.count) * u64::from(self.bytes)
        );
        for _ in 0..self.count {
            let receipt = self
                .funding
                .split(u64::from(self.bytes))
                .expect("each receipt is funded");
            drop(receipt);
        }
        assert_eq!(self.budget.reserved(), 0);
    }
}

/// Repeated peer requests over one queue and one capacity budget.
#[derive(Clone)]
pub struct BenchWorkChurn {
    work: Arc<WorkQueue>,
    budget: ByteBudget,
    peers: u32,
    count: u32,
    bytes: u32,
}

impl BenchWorkChurn {
    /// Retain the actual budget's capacity signal independently of a peer handle.
    pub fn capacity_reader(&self) -> BenchWorkCapacity {
        BenchWorkCapacity(self.budget.clone())
    }

    /// Give each peer a disjoint range above the floor request.
    pub fn new(peers: u32, count: u32, bytes: u32, active_slots: u32) -> Self {
        assert!((1..=128).contains(&peers) && (1..=128).contains(&count));
        assert!((1..=peers).contains(&active_slots));
        let limit = u64::from(active_slots) * u64::from(count) * u64::from(bytes);
        let (work, budget) = pending_work(peers * count + 1, bytes, limit);
        Self {
            work,
            budget,
            peers,
            count,
            bytes,
        }
    }

    /// Publish and drop one queued request, or return its take on capacity refusal.
    /// The caller can hold the published owner to exercise overlapping requests.
    pub fn cycle(&mut self, peer: u32, attempt: NonZeroU64, held: impl FnOnce()) -> bool {
        assert!(peer < self.peers);
        let low = peer * self.count + 2;
        let taken = self.work.take_request(
            block::Height(low),
            block::Height(low + self.count - 1),
            usize::try_from(self.count).expect("request count fits usize"),
            u64::from(self.count) * u64::from(self.bytes),
            u64::from(peer) + 1,
            attempt,
        );
        assert_eq!(taken.items().len(), usize::try_from(self.count).unwrap());
        let admitted = match taken.admit(block::Height(0), CancellationToken::new()) {
            Ok(admitted) => admitted,
            Err(AdmitRefusal::Capacity) => return false,
            Err(other) => panic!("churn admission failed unexpectedly: {other:?}"),
        };
        let claim = admitted
            .publish(|_| {})
            .expect("the peer owns its request range");
        held();
        drop(claim);
        true
    }

    /// Inspect all obligations before destroying the shared queue.
    pub fn assert_settled(&self) {
        assert_eq!(self.budget.reserved(), 0);
        assert_eq!(self.work.in_flight_len(), 0);
        assert_eq!(
            self.work.pending_len(),
            usize::try_from(self.peers * self.count + 1).unwrap()
        );
        assert!(!self.budget.is_closed());
    }
}

/// Capacity observations for the benchmark's production admission budget.
pub struct BenchWorkCapacity(ByteBudget);

impl BenchWorkCapacity {
    /// Register before admission so a concurrent release cannot be missed.
    pub fn subscribe(&self) -> &zakura_rt::sync::Notify {
        self.0.subscribe_capacity()
    }

    /// Observe capacity after a refusal; this observation grants no reservation.
    pub fn available(&self) -> u64 {
        self.0.available()
    }
}
