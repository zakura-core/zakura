//! Shared decoded-block retention across ingress, verification, and state storage.
//!
//! Parsing has a separate transient allowance. A pool admits a parsed block before
//! a caller retains it. Block and transaction handles share the same allocation
//! and charge. A backend transfer reserves its destination before releasing the
//! source charge, including when other handles still retain the allocation.

use std::{fmt, io, num::NonZeroU64, ops::Deref, sync::Arc};

use zakura_chain::{
    block::{self, Block, BlockView, Header},
    serialization::ZcashSerialize,
    transaction::Transaction,
};
use zakura_rt::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Mutex, Notify,
};

#[cfg(any(test, zakura_sim, feature = "test-support"))]
pub mod model;

/// Capacity for decoded blocks, including references retained outside their store.
#[derive(Clone, Debug)]
pub struct DecodedBlockPool(Arc<Pool>);

#[derive(Debug)]
struct Pool {
    limit: RetentionLimit,
    reserved: AtomicU64,
    counted_bytes: AtomicU64,
    capacity_version: AtomicU64,
    closed: AtomicBool,
    capacity: Notify,
}

#[derive(Clone, Copy, Debug)]
enum RetentionLimit {
    Bytes(u64),
    Blocks(NonZeroU64),
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
    /// Attributed bytes measured during this admission attempt.
    pub decoded_bytes: u64,
}

impl DecodedBlockPool {
    /// Create one immutable byte-capacity generation.
    pub fn new(limit: u64) -> Self {
        Self::with_limit(RetentionLimit::Bytes(limit))
    }

    /// Enforce a block-count allowance while tracking its actual decoded bytes.
    /// Checkpoint and backend owners can preserve their existing count contracts.
    /// This constructor does not establish a decoded-byte bound.
    pub fn for_block_count(limit: NonZeroU64) -> Self {
        Self::with_limit(RetentionLimit::Blocks(limit))
    }

    fn with_limit(limit: RetentionLimit) -> Self {
        Self(Arc::new(Pool {
            limit,
            reserved: AtomicU64::new(0),
            counted_bytes: AtomicU64::new(0),
            capacity_version: AtomicU64::new(0),
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
        let bytes = block.attributed_memory_size_bytes();
        if Arc::get_mut(&mut block.header).is_none()
            || block
                .transactions
                .iter_mut()
                .any(|transaction| Arc::get_mut(transaction).is_none())
        {
            return Err(RejectedBlock {
                reason: AdmissionFailure::AliasedPayload,
                block,
                decoded_bytes: bytes,
            });
        }
        let charge = match self.reserve(bytes, admission) {
            Ok(charge) => charge,
            Err(reason) => {
                return Err(RejectedBlock {
                    reason,
                    block,
                    decoded_bytes: bytes,
                })
            }
        };
        Ok(OwnedBlock(Arc::new(RetainedBlock {
            block,
            charge: Mutex::new(charge),
            bytes,
        })))
    }

    /// Current attributed decoded bytes, including detached holders.
    pub fn reserved_bytes(&self) -> u64 {
        match self.0.limit {
            RetentionLimit::Bytes(_) => self.0.reserved.load(Ordering::Acquire),
            RetentionLimit::Blocks(_) => self.0.counted_bytes.load(Ordering::Acquire),
        }
    }

    /// Current retained allocations when this pool enforces a block-count allowance.
    pub fn reserved_blocks(&self) -> Option<u64> {
        match self.0.limit {
            RetentionLimit::Bytes(_) => None,
            RetentionLimit::Blocks(_) => Some(self.0.reserved.load(Ordering::Acquire)),
        }
    }

    /// Whether this generation rejects all further admission.
    pub fn is_closed(&self) -> bool {
        self.0.closed.load(Ordering::Acquire)
    }

    /// Permanently close admission without erasing existing charges.
    pub fn close(&self) {
        self.0.closed.store(true, Ordering::Release);
        self.notify_capacity();
    }

    /// Wait until this immutable generation closes admission.
    pub async fn closed(&self) {
        loop {
            let changed = self.capacity().notified();
            if self.is_closed() {
                return;
            }
            changed.await;
        }
    }

    /// Version of capacity releases and closure. Read before trying admission.
    /// After registering a waiter, retry if this version changed in the gap.
    pub fn capacity_version(&self) -> u64 {
        self.0.capacity_version.load(Ordering::Acquire)
    }

    fn notify_capacity(&self) {
        if self
            .0
            .capacity_version
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |version| {
                version.checked_add(1)
            })
            .is_err()
        {
            self.0.closed.store(true, Ordering::Release);
        }
        self.0.capacity.notify_waiters();
    }

    /// Register a notification before retrying admission, then await it on refusal.
    pub fn capacity(&self) -> &Notify {
        &self.0.capacity
    }

    fn reserve(&self, bytes: u64, admission: Admission) -> Result<Charge, AdmissionFailure> {
        let (units, limit) = match self.0.limit {
            RetentionLimit::Bytes(limit) => (bytes, limit),
            RetentionLimit::Blocks(limit) => (1, limit.get()),
        };
        let mut used = self.0.reserved.load(Ordering::Acquire);
        loop {
            if self.is_closed() {
                return Err(AdmissionFailure::Closed);
            }
            let Some(next) = used.checked_add(units) else {
                return Err(AdmissionFailure::Capacity);
            };
            if next > limit && !(used == 0 && admission == Admission::SingleOversized) {
                return Err(AdmissionFailure::Capacity);
            }
            match self.0.reserved.compare_exchange_weak(
                used,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    if matches!(self.0.limit, RetentionLimit::Blocks(_))
                        && self
                            .0
                            .counted_bytes
                            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                                used.checked_add(bytes)
                            })
                            .is_err()
                    {
                        self.close();
                        let _ = self.0.reserved.fetch_update(
                            Ordering::AcqRel,
                            Ordering::Acquire,
                            |used| used.checked_sub(units),
                        );
                        return Err(AdmissionFailure::Closed);
                    }
                    let charge = Charge {
                        pool: self.clone(),
                        bytes,
                        units,
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
    units: u64,
}

impl Drop for Charge {
    fn drop(&mut self) {
        if matches!(self.pool.0.limit, RetentionLimit::Blocks(_))
            && self
                .pool
                .0
                .counted_bytes
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                    used.checked_sub(self.bytes)
                })
                .is_err()
        {
            self.pool.close();
            return;
        }
        if self
            .pool
            .0
            .reserved
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_sub(self.units)
            })
            .is_err()
        {
            self.pool.close();
            return;
        }
        self.pool.notify_capacity();
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
///
/// Borrowed views cannot return a raw shared transaction handle.
///
/// ```compile_fail
/// use std::sync::Arc;
/// use zakura_chain::{block::BlockView, transaction::Transaction};
/// use zakura_node_services::block_memory::OwnedBlock;
/// fn escape(owner: &OwnedBlock) -> Arc<Transaction> {
///     BlockView::from(owner).transactions().next().unwrap().clone()
/// }
/// ```
#[derive(Clone, Debug)]
pub struct OwnedBlock(Arc<RetainedBlock>);

impl<'a> From<&'a OwnedBlock> for BlockView<'a> {
    fn from(block: &'a OwnedBlock) -> Self {
        (&block.0.block).into()
    }
}

impl ZcashSerialize for OwnedBlock {
    fn zcash_serialize<W: io::Write + ?Sized>(&self, writer: &mut W) -> Result<(), io::Error> {
        self.0.block.zcash_serialize(writer)
    }
}

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

/// A retained block passed between verification, state, and readers.
#[derive(Clone, Debug)]
pub enum BlockHandle {
    /// A caller whose admission has not yet migrated to decoded-block ownership.
    Shared(Arc<Block>),
    /// A block whose escaping handles retain its decoded-memory charge.
    Owned(OwnedBlock),
}

impl BlockHandle {
    // Only this module may borrow the raw Arc-bearing payload.
    fn block(&self) -> &Block {
        match self {
            Self::Shared(block) => block,
            Self::Owned(block) => &block.0.block,
        }
    }

    /// Borrow payloads without exposing raw shared handles.
    pub fn view(&self) -> BlockView<'_> {
        self.block().into()
    }

    /// Borrow the block header.
    pub fn header(&self) -> &Header {
        self.view().header()
    }

    /// Retain the header with its existing ownership.
    pub fn retained_header(&self) -> HeaderHandle {
        match self {
            Self::Shared(block) => HeaderHandle::Shared(block.header.clone()),
            Self::Owned(block) => HeaderHandle::Block(block.clone()),
        }
    }

    /// Retain the indexed transaction with its existing ownership.
    pub fn transaction(&self, index: usize) -> Option<TransactionHandle> {
        match self {
            Self::Shared(block) => block.transactions.get(index).cloned().map(Into::into),
            Self::Owned(block) => block.transaction(index).map(Into::into),
        }
    }

    /// Retain each transaction with its existing ownership, in block order.
    pub fn transactions(
        &self,
    ) -> impl ExactSizeIterator<Item = TransactionHandle> + DoubleEndedIterator + '_ {
        (0..self.block().transactions.len()).map(|index| {
            self.transaction(index)
                .expect("the index comes from this immutable block")
        })
    }

    /// Return the height reported by the coinbase transaction.
    pub fn coinbase_height(&self) -> Option<block::Height> {
        self.block().coinbase_height()
    }

    /// Compute the block hash.
    pub fn hash(&self) -> block::Hash {
        self.block().hash()
    }

    /// Compute the authorizing-data commitment root.
    pub fn auth_data_root(&self) -> block::merkle::AuthDataRoot {
        self.block().auth_data_root()
    }
}

impl<'a> From<&'a BlockHandle> for BlockView<'a> {
    fn from(block: &'a BlockHandle) -> Self {
        block.view()
    }
}

impl From<Arc<Block>> for BlockHandle {
    fn from(block: Arc<Block>) -> Self {
        Self::Shared(block)
    }
}

impl From<OwnedBlock> for BlockHandle {
    fn from(block: OwnedBlock) -> Self {
        Self::Owned(block)
    }
}

impl fmt::Display for BlockHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.block().fmt(formatter)
    }
}

impl PartialEq for BlockHandle {
    fn eq(&self, other: &Self) -> bool {
        self.block() == other.block()
    }
}

impl Eq for BlockHandle {}

impl ZcashSerialize for BlockHandle {
    fn zcash_serialize<W: io::Write + ?Sized>(&self, writer: &mut W) -> Result<(), io::Error> {
        self.block().zcash_serialize(writer)
    }
}

/// A header retained independently of its originating block handle.
#[derive(Clone, Debug)]
pub enum HeaderHandle {
    /// A header whose admission belongs to a separate header pipeline.
    Shared(Arc<Header>),
    /// A header that still retains its block's decoded-memory charge.
    Block(OwnedBlock),
}

impl Deref for HeaderHandle {
    type Target = Header;

    fn deref(&self) -> &Header {
        match self {
            Self::Shared(header) => header,
            Self::Block(block) => &block.0.block.header,
        }
    }
}

impl AsRef<Header> for HeaderHandle {
    fn as_ref(&self) -> &Header {
        self
    }
}

impl std::borrow::Borrow<Header> for HeaderHandle {
    fn borrow(&self) -> &Header {
        self
    }
}

impl PartialEq for HeaderHandle {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref() == other.as_ref()
    }
}

impl Eq for HeaderHandle {}

impl From<Arc<Header>> for HeaderHandle {
    fn from(header: Arc<Header>) -> Self {
        Self::Shared(header)
    }
}

impl ZcashSerialize for HeaderHandle {
    fn zcash_serialize<W: io::Write + ?Sized>(&self, writer: &mut W) -> Result<(), io::Error> {
        self.as_ref().zcash_serialize(writer)
    }
}

/// The payload retained by transaction verification and its detached workers.
#[derive(Clone, Debug)]
pub enum TransactionHandle {
    /// A caller whose memory admission has not yet migrated to block ownership.
    Shared(Arc<Transaction>),
    /// A transaction that retains its block's decoded-memory charge.
    Owned(OwnedBlockTransaction),
}

impl Deref for TransactionHandle {
    type Target = Transaction;

    fn deref(&self) -> &Transaction {
        match self {
            Self::Shared(transaction) => transaction,
            Self::Owned(transaction) => transaction,
        }
    }
}

impl AsRef<Transaction> for TransactionHandle {
    fn as_ref(&self) -> &Transaction {
        self
    }
}

impl std::borrow::Borrow<Transaction> for TransactionHandle {
    fn borrow(&self) -> &Transaction {
        self
    }
}

impl PartialEq for TransactionHandle {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref() == other.as_ref()
    }
}

impl Eq for TransactionHandle {}

impl From<Arc<Transaction>> for TransactionHandle {
    fn from(transaction: Arc<Transaction>) -> Self {
        Self::Shared(transaction)
    }
}

impl From<OwnedBlockTransaction> for TransactionHandle {
    fn from(transaction: OwnedBlockTransaction) -> Self {
        Self::Owned(transaction)
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
    fn escaping_block_parts_keep_the_original_charge() {
        let block = block();
        let bytes = block.attributed_memory_size_bytes();
        let pool = DecodedBlockPool::new(bytes);
        let owner = pool.try_admit(block, Admission::WithinLimit).unwrap();
        let actual = Arc::downgrade(&owner.0.block.transactions[0]);
        let handle = BlockHandle::from(owner);
        let header = handle.retained_header();
        let transaction = handle.transaction(0).unwrap();
        assert!(matches!(header, HeaderHandle::Block(_)));
        assert!(matches!(transaction, TransactionHandle::Owned(_)));
        assert_eq!(handle.transactions().len(), 1);
        drop(handle);
        assert_eq!(pool.reserved_bytes(), bytes);
        drop(transaction);
        assert_eq!(pool.reserved_bytes(), bytes);
        assert!(actual.upgrade().is_some());
        drop(header);
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
    fn block_count_admission_retains_bytes_until_the_last_holder() {
        let pool = DecodedBlockPool::for_block_count(NonZeroU64::MIN);
        let bytes = block().attributed_memory_size_bytes();
        let owner = pool.try_admit(block(), Admission::WithinLimit).unwrap();
        let actual = Arc::downgrade(&owner.0.block.transactions[0]);
        let retained = owner.transaction(0).unwrap();
        let version = pool.capacity_version();
        assert_eq!(pool.reserved_blocks(), Some(1));
        assert_eq!(pool.reserved_bytes(), bytes);
        assert_eq!(
            pool.try_admit(block(), Admission::SingleOversized)
                .unwrap_err()
                .reason,
            AdmissionFailure::Capacity
        );
        drop(owner);
        assert!(actual.upgrade().is_some());
        assert_eq!(pool.reserved_blocks(), Some(1));
        assert_eq!(pool.capacity_version(), version);
        drop(retained);
        assert!(actual.upgrade().is_none());
        assert_eq!(pool.reserved_blocks(), Some(0));
        assert_eq!(pool.reserved_bytes(), 0);
        assert_ne!(
            pool.capacity_version(),
            version,
            "a release before waiter registration must remain observable"
        );
        let replacement = pool.try_admit(block(), Admission::WithinLimit).unwrap();
        assert_eq!(pool.reserved_bytes(), bytes);
        drop(replacement);
    }

    #[test]
    fn transfers_between_byte_and_count_allowances_keep_one_owner() {
        let bytes = block().attributed_memory_size_bytes();
        let byte_pool = DecodedBlockPool::new(bytes);
        let count_pool = DecodedBlockPool::for_block_count(NonZeroU64::MIN);
        let owner = byte_pool
            .try_admit(block(), Admission::WithinLimit)
            .unwrap();
        let transaction = owner.transaction(0).unwrap();
        owner
            .transfer_to(&count_pool, Admission::WithinLimit)
            .unwrap();
        assert_eq!(byte_pool.reserved_bytes(), 0);
        assert_eq!(count_pool.reserved_bytes(), bytes);
        assert_eq!(count_pool.reserved_blocks(), Some(1));
        let blocker = byte_pool
            .try_admit(block(), Admission::WithinLimit)
            .unwrap();
        assert_eq!(
            owner.transfer_to(&byte_pool, Admission::WithinLimit),
            Err(AdmissionFailure::Capacity)
        );
        assert_eq!(count_pool.reserved_blocks(), Some(1));
        drop(blocker);
        owner
            .transfer_to(&byte_pool, Admission::WithinLimit)
            .unwrap();
        assert_eq!(count_pool.reserved_blocks(), Some(0));
        assert_eq!(count_pool.reserved_bytes(), 0);
        assert_eq!(byte_pool.reserved_bytes(), bytes);
        drop(owner);
        assert_eq!(byte_pool.reserved_bytes(), bytes);
        drop(transaction);
        assert_eq!(byte_pool.reserved_bytes(), 0);
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
