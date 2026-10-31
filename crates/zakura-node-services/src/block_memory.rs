//! Shared decoded-block retention across ingress, verification, and state storage.
//!
//! Parsing has a separate transient allowance. A pool admits a parsed block before
//! a caller retains it. Block and transaction handles share the same allocation
//! and charge. A backend transfer reserves its destination before releasing the
//! source charge, including when other handles still retain the allocation.

use std::{fmt, ops::Deref, sync::Arc};

use zakura_chain::{block::Block, transaction::Transaction};
use zakura_rt::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Mutex, Notify,
};

/// Capacity for decoded blocks, including references retained outside their store.
#[derive(Clone, Debug)]
pub struct DecodedBlockPool(Arc<Pool>);

#[derive(Debug)]
struct Pool {
    limit: u64,
    reserved: AtomicU64,
    closed: AtomicBool,
    capacity: Notify,
}

/// Admission rule selected by the owning pipeline.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Admission {
    /// Admit only within the pool's configured limit.
    WithinLimit,
    /// Permit one oversized block only while this pool has no other charge.
    SingleOversized,
}

/// Why a decoded block cannot enter or transfer between pools.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmissionFailure {
    /// A header or transaction has a holder outside the proposed owner.
    AliasedPayload,
    /// Existing holders or the block's size leave insufficient capacity.
    Capacity,
    /// This pool generation has permanently closed admission.
    Closed,
    /// An interrupted transfer poisoned the block's charge owner.
    Poisoned,
}

impl fmt::Display for AdmissionFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::AliasedPayload => "decoded block contains shared payloads",
            Self::Capacity => "decoded block capacity exhausted",
            Self::Closed => "decoded block pool closed",
            Self::Poisoned => "decoded block charge owner poisoned",
        })
    }
}

impl std::error::Error for AdmissionFailure {}

/// A refused parsed block remains with its caller's transient decode scope.
#[derive(Debug)]
pub struct RejectedBlock {
    /// Admission failure that left the pool unchanged.
    pub reason: AdmissionFailure,
    /// The parsed block, which did not acquire retained-memory ownership.
    pub block: Block,
}

impl DecodedBlockPool {
    /// Create one immutable capacity generation.
    pub fn new(limit: u64) -> Self {
        Self(Arc::new(Pool {
            limit,
            reserved: AtomicU64::new(0),
            closed: AtomicBool::new(false),
            capacity: Notify::new(),
        }))
    }

    /// Admit an exclusively held block before allocating its shared owner.
    /// Existing strong or weak header/transaction aliases prevent adoption.
    pub fn try_admit(
        &self,
        mut block: Block,
        admission: Admission,
    ) -> Result<OwnedBlock, RejectedBlock> {
        if Arc::get_mut(&mut block.header).is_none()
            || block
                .transactions
                .iter_mut()
                .any(|transaction| Arc::get_mut(transaction).is_none())
        {
            return Err(RejectedBlock {
                reason: AdmissionFailure::AliasedPayload,
                block,
            });
        }
        let bytes = block.attributed_memory_size_bytes();
        let charge = match self.reserve(bytes, admission) {
            Ok(charge) => charge,
            Err(reason) => return Err(RejectedBlock { reason, block }),
        };
        Ok(OwnedBlock(Arc::new(RetainedBlock {
            block,
            charge: Mutex::new(charge),
            bytes,
        })))
    }

    /// Current attributed decoded bytes, including detached holders.
    pub fn reserved_bytes(&self) -> u64 {
        self.0.reserved.load(Ordering::Acquire)
    }

    /// Whether this generation rejects all further admission.
    pub fn is_closed(&self) -> bool {
        self.0.closed.load(Ordering::Acquire)
    }

    /// Permanently close admission without erasing existing charges.
    pub fn close(&self) {
        self.0.closed.store(true, Ordering::Release);
        self.0.capacity.notify_waiters();
    }

    /// Register a notification before retrying admission, then await it on refusal.
    pub fn capacity(&self) -> &Notify {
        &self.0.capacity
    }

    fn reserve(&self, bytes: u64, admission: Admission) -> Result<Charge, AdmissionFailure> {
        let mut used = self.reserved_bytes();
        loop {
            if self.is_closed() {
                return Err(AdmissionFailure::Closed);
            }
            let Some(next) = used.checked_add(bytes) else {
                return Err(AdmissionFailure::Capacity);
            };
            if next > self.0.limit && !(used == 0 && admission == Admission::SingleOversized) {
                return Err(AdmissionFailure::Capacity);
            }
            match self.0.reserved.compare_exchange_weak(
                used,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    let charge = Charge {
                        pool: self.clone(),
                        bytes,
                    };
                    if self.is_closed() {
                        drop(charge);
                        return Err(AdmissionFailure::Closed);
                    }
                    return Ok(charge);
                }
                Err(current) => used = current,
            }
        }
    }
}

#[derive(Debug)]
struct Charge {
    pool: DecodedBlockPool,
    bytes: u64,
}

impl Drop for Charge {
    fn drop(&mut self) {
        let mut used = self.pool.reserved_bytes();
        loop {
            let Some(next) = used.checked_sub(self.bytes) else {
                self.pool.close();
                return;
            };
            match self.pool.0.reserved.compare_exchange_weak(
                used,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(current) => used = current,
            }
        }
        self.pool.0.capacity.notify_waiters();
    }
}

#[derive(Debug)]
struct RetainedBlock {
    // Fields drop in declaration order: payload destruction precedes settlement.
    block: Block,
    charge: Mutex<Charge>,
    bytes: u64,
}

/// A shared block whose last block or transaction handle settles its decoded charge.
///
/// The owner does not expose the block's cloneable transaction Arcs.
///
/// ```compile_fail
/// use zakura_node_services::block_memory::OwnedBlock;
/// fn escape(owner: OwnedBlock) {
///     let transaction = owner.transactions[0].clone();
///     drop(owner);
///     drop(transaction);
/// }
/// ```
#[derive(Clone, Debug)]
pub struct OwnedBlock(Arc<RetainedBlock>);

impl OwnedBlock {
    /// Decoded size measured once, before retained admission.
    pub fn decoded_bytes(&self) -> u64 {
        self.0.bytes
    }

    /// Retain one transaction through verification or a detached worker.
    /// The handle keeps the block's entire decoded charge until it drops.
    pub fn transaction(&self, index: usize) -> Option<OwnedBlockTransaction> {
        self.0.block.transactions.get(index)?;
        Some(OwnedBlockTransaction {
            block: self.clone(),
            index,
        })
    }

    /// Move the allocation's shared charge to a backend pool.
    /// All existing handles follow this transfer. Refusal preserves the source.
    pub fn transfer_to(
        &self,
        destination: &DecodedBlockPool,
        admission: Admission,
    ) -> Result<(), AdmissionFailure> {
        let mut current = match self.0.charge.lock() {
            Ok(current) => current,
            Err(poisoned) => {
                let source = poisoned.get_ref().pool.clone();
                drop(poisoned);
                source.close();
                return Err(AdmissionFailure::Poisoned);
            }
        };
        if Arc::ptr_eq(&current.pool.0, &destination.0) {
            return Ok(());
        }
        let next = destination.reserve(self.0.bytes, admission)?;
        let previous = std::mem::replace(&mut *current, next);
        drop(current);
        drop(previous);
        Ok(())
    }
}

/// A transaction handle that cannot outlive its block's decoded charge.
#[derive(Clone, Debug)]
pub struct OwnedBlockTransaction {
    block: OwnedBlock,
    index: usize,
}

impl Deref for OwnedBlockTransaction {
    type Target = Transaction;

    fn deref(&self) -> &Transaction {
        &self.block.0.block.transactions[self.index]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zakura_chain::serialization::ZcashDeserialize;

    fn block() -> Block {
        Block::zcash_deserialize(&zakura_test::vectors::BLOCK_MAINNET_1_BYTES[..]).unwrap()
    }

    #[test]
    fn transaction_handle_keeps_the_allocation_and_charge() {
        let block = block();
        let bytes = block.attributed_memory_size_bytes();
        let pool = DecodedBlockPool::new(bytes);
        let owner = pool.try_admit(block, Admission::WithinLimit).unwrap();
        let actual = Arc::downgrade(&owner.0.block.transactions[0]);
        let transaction = owner.transaction(0).unwrap();
        assert!(owner.transaction(usize::MAX).is_none());
        drop(owner);
        assert_eq!(pool.reserved_bytes(), bytes);
        assert!(actual.upgrade().is_some());
        drop(transaction);
        assert_eq!(pool.reserved_bytes(), 0);
        assert!(actual.upgrade().is_none());
    }

    #[test]
    fn admission_rejects_external_payload_aliases_without_charging() {
        let pool = DecodedBlockPool::new(u64::MAX);
        let original = block();
        let alias = original.transactions[0].clone();
        let rejected = pool
            .try_admit(original, Admission::WithinLimit)
            .unwrap_err();
        assert_eq!(rejected.reason, AdmissionFailure::AliasedPayload);
        assert_eq!(pool.reserved_bytes(), 0);
        drop(alias);
        let weak = Arc::downgrade(&rejected.block.header);
        let rejected = pool
            .try_admit(rejected.block, Admission::WithinLimit)
            .unwrap_err();
        assert_eq!(rejected.reason, AdmissionFailure::AliasedPayload);
        assert_eq!(pool.reserved_bytes(), 0);
        drop(weak);
        let owner = pool
            .try_admit(rejected.block, Admission::WithinLimit)
            .unwrap();
        assert_eq!(pool.reserved_bytes(), owner.decoded_bytes());
        drop(owner);
        assert_eq!(pool.reserved_bytes(), 0);
    }

    #[test]
    fn transfer_refusal_preserves_both_pools_and_success_preserves_other_owners() {
        let bytes = block().attributed_memory_size_bytes();
        let source = DecodedBlockPool::new(2 * bytes);
        let destination = DecodedBlockPool::new(bytes);
        let owner = source.try_admit(block(), Admission::WithinLimit).unwrap();
        let other = source.try_admit(block(), Admission::WithinLimit).unwrap();
        let blocker = destination
            .try_admit(block(), Admission::WithinLimit)
            .unwrap();
        assert_eq!(
            owner.transfer_to(&destination, Admission::WithinLimit),
            Err(AdmissionFailure::Capacity)
        );
        assert_eq!(source.reserved_bytes(), 2 * bytes);
        assert_eq!(destination.reserved_bytes(), bytes);
        drop(blocker);
        let copy = owner.clone();
        owner
            .transfer_to(&destination, Admission::WithinLimit)
            .unwrap();
        owner
            .transfer_to(&destination, Admission::WithinLimit)
            .unwrap();
        assert_eq!(source.reserved_bytes(), bytes);
        assert_eq!(destination.reserved_bytes(), bytes);
        drop(owner);
        assert_eq!(destination.reserved_bytes(), bytes);
        drop(copy);
        assert_eq!(destination.reserved_bytes(), 0);
        assert_eq!(source.reserved_bytes(), bytes);
        drop(other);
        assert_eq!(source.reserved_bytes(), 0);
    }

    #[test]
    fn simultaneous_transfers_leave_one_charge() {
        let bytes = block().attributed_memory_size_bytes();
        let source = DecodedBlockPool::new(bytes);
        let destinations = [DecodedBlockPool::new(bytes), DecodedBlockPool::new(bytes)];
        let owner = source.try_admit(block(), Admission::WithinLimit).unwrap();
        let start = std::sync::Barrier::new(2);
        std::thread::scope(|threads| {
            for destination in &destinations {
                let owner = &owner;
                let start = &start;
                threads.spawn(move || {
                    start.wait();
                    owner
                        .transfer_to(destination, Admission::WithinLimit)
                        .unwrap();
                });
            }
        });
        assert_eq!(source.reserved_bytes(), 0);
        assert_eq!(
            destinations
                .iter()
                .map(DecodedBlockPool::reserved_bytes)
                .sum::<u64>(),
            bytes
        );
        drop(owner);
        assert!(destinations.iter().all(|pool| pool.reserved_bytes() == 0));
    }

    #[test]
    fn oversized_admission_is_exclusive_under_competing_callers() {
        let bytes = block().attributed_memory_size_bytes();
        let pool = DecodedBlockPool::new(bytes - 1);
        assert_eq!(
            pool.try_admit(block(), Admission::WithinLimit)
                .unwrap_err()
                .reason,
            AdmissionFailure::Capacity
        );
        let barrier = std::sync::Barrier::new(2);
        let admitted = std::sync::atomic::AtomicUsize::new(0);
        let observations = std::sync::Mutex::new(Vec::new());
        std::thread::scope(|threads| {
            for _ in 0..2 {
                threads.spawn(|| {
                    let block = block();
                    barrier.wait();
                    let owner = pool.try_admit(block, Admission::SingleOversized).ok();
                    admitted.fetch_add(usize::from(owner.is_some()), Ordering::Relaxed);
                    barrier.wait();
                    observations.lock().unwrap().push(pool.reserved_bytes());
                    barrier.wait();
                    drop(owner);
                });
            }
        });
        assert_eq!(admitted.load(Ordering::Relaxed), 1);
        assert_eq!(*observations.lock().unwrap(), [bytes, bytes]);
        assert_eq!(pool.reserved_bytes(), 0);
    }

    #[tokio::test]
    async fn closing_and_last_release_wake_registered_waiters() {
        let bytes = block().attributed_memory_size_bytes();
        let pool = DecodedBlockPool::new(bytes);
        let owner = pool.try_admit(block(), Admission::WithinLimit).unwrap();
        let closed = pool.capacity().notified();
        tokio::pin!(closed);
        closed.as_mut().enable();
        pool.close();
        tokio::time::timeout(std::time::Duration::from_secs(1), closed)
            .await
            .unwrap();
        assert_eq!(pool.reserved_bytes(), bytes);
        assert_eq!(
            pool.try_admit(block(), Admission::SingleOversized)
                .unwrap_err()
                .reason,
            AdmissionFailure::Closed
        );
        let released = pool.capacity().notified();
        tokio::pin!(released);
        released.as_mut().enable();
        drop(owner);
        tokio::time::timeout(std::time::Duration::from_secs(1), released)
            .await
            .unwrap();
        assert!(pool.is_closed());
        assert_eq!(pool.reserved_bytes(), 0);
    }

    #[test]
    fn underflow_closes_admission_without_erasing_the_counter() {
        let bytes = block().attributed_memory_size_bytes();
        let pool = DecodedBlockPool::new(bytes);
        let owner = pool.try_admit(block(), Admission::WithinLimit).unwrap();
        pool.0.reserved.store(bytes - 1, Ordering::Release);
        drop(owner);
        assert!(pool.is_closed());
        assert_eq!(pool.reserved_bytes(), bytes - 1);
        assert_eq!(
            pool.try_admit(block(), Admission::WithinLimit)
                .unwrap_err()
                .reason,
            AdmissionFailure::Closed
        );
    }

    #[test]
    fn poisoned_transfer_closes_source_and_preserves_its_live_charge() {
        let bytes = block().attributed_memory_size_bytes();
        let source = DecodedBlockPool::new(bytes);
        let destination = DecodedBlockPool::new(bytes);
        let owner = source.try_admit(block(), Admission::WithinLimit).unwrap();
        let interrupted = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _charge = owner.0.charge.lock().unwrap();
            panic!("interrupt the transfer owner");
        }));
        assert!(interrupted.is_err());
        assert_eq!(
            owner.transfer_to(&destination, Admission::WithinLimit),
            Err(AdmissionFailure::Poisoned)
        );
        assert!(source.is_closed());
        assert_eq!(source.reserved_bytes(), bytes);
        assert_eq!(destination.reserved_bytes(), 0);
        drop(owner);
        assert_eq!(source.reserved_bytes(), 0);
        assert!(source.is_closed());
    }
}
