//! Decoded payload retention across ingress, verification, and state storage.
//!
//! Block ingress reserves parser transients separately from retained admission.
//! Block and derived transaction handles share one allocation and charge.
//! A backend transfer reserves its destination before releasing the source charge.
//! Disk reads reserve count admission before materialization. Standalone
//! results keep their own slot through their final holder. A count bounds bytes
//! only for payloads with a fixed maximum size, such as headers and serialized
//! blocks. [`ReadMemoryBudget`] bounds what a count cannot: parser transients and
//! the decoded bytes of variable-size results, under two separate limits.
//!
//! # Capacity and progress contract
//!
//! Every admission in this module refuses instead of waiting, so no caller can
//! wait for capacity while it holds a lock, a worker slot, or an earlier reservation.
//!
//! | Actor | Contract |
//! | --- | --- |
//! | Waiting caller | Waits for a worker slot before it holds any lock or memory charge. Dropping the wait releases the request. |
//! | Reader | Acquires its result slot, parses under a [`ParseScope`], then funds the retained charge before it releases the parser charge. A refusal at any step releases every earlier step. |
//! | Snapshot | Holds its retained handles for one read job. The job's end releases them, so a snapshot cannot hold capacity for an idle peer. |
//! | Fork | A block handle keeps its charge after its chain leaves the non-finalized state. Its last holder settles the charge. |
//! | Writer | Takes no ordinary result capacity. Chain-tip publication uses a fixed partition, so readers cannot stall a commit. |
//! | Rayon work | Holds no shared lock across a wait for its pool. A detached verifier job owns its payload handle until the job drops it. |
//!
//! A reservation always succeeds when its gauge has no other holder, even above the
//! limit. Refusal therefore proves that another holder exists and that its release
//! allows a retry. This bounds each gauge by its limit plus one legal payload.
//!
//! Disk retention has no representation here. Store policies own durable limits,
//! and a memory lease says nothing about what a commit wrote.

use std::{
    fmt, io,
    num::{NonZeroU16, NonZeroU64},
    ops::Deref,
    sync::Arc,
};

use zakura_chain::{
    block::{self, Block, BlockView, Header},
    serialization::ZcashSerialize,
    transaction::Transaction,
};
use zakura_rt::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Mutex, Notify, OwnedSemaphorePermit, Semaphore,
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
    /// Pre-decode admission requires a block-count pool, not a byte pool.
    DecodeRequiresCountLimit,
}

impl fmt::Display for AdmissionFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::AliasedPayload => "decoded block contains shared payloads",
            Self::Capacity => "decoded block capacity exhausted",
            Self::Closed => "decoded block pool closed",
            Self::Poisoned => "decoded block charge owner poisoned",
            Self::DecodeRequiresCountLimit => "pre-decode admission requires a block-count pool",
        })
    }
}

impl std::error::Error for AdmissionFailure {}

impl From<ReadMemoryExhausted> for AdmissionFailure {
    fn from(_: ReadMemoryExhausted) -> Self {
        Self::Capacity
    }
}

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

    /// Reserve one block before reading or decoding it, then retain that reservation
    /// through its final holder. Missing data and unwinding release the reservation.
    ///
    /// Only count-limited pools support this operation. The count includes active
    /// decoders and returned blocks. Decoded bytes are measured after materialization.
    /// Use this only where a fixed count is the memory bound, such as a small partition.
    /// [`Self::try_decode_bounded`] also enforces parser and decoded-byte limits.
    pub fn try_decode(
        &self,
        decode: impl FnOnce() -> Option<Block>,
    ) -> Result<Option<OwnedBlock>, AdmissionFailure> {
        self.decode_admitted(|| Ok(decode()), |_| Ok(None))
    }

    /// Decode like [`Self::try_decode`], and also charge `memory`.
    ///
    /// The steps run in this order: reserve the count, parse under `decode`'s scope,
    /// measure the block, fund its retained bytes, and release the parser charge.
    /// The retained charge follows the block to its final holder, including after a
    /// transfer to another pool. Refusal and unwinding release every earlier step.
    pub fn try_decode_bounded(
        &self,
        memory: &ReadMemoryBudget,
        decode: impl FnOnce(&mut ParseScope) -> Result<Option<Block>, ReadMemoryExhausted>,
    ) -> Result<Option<OwnedBlock>, AdmissionFailure> {
        let mut parsing = memory.parse_scope();
        self.decode_admitted(
            || Ok(decode(&mut parsing)?),
            |bytes| Ok(Some(memory.retain(bytes)?)),
        )
    }

    fn decode_admitted(
        &self,
        decode: impl FnOnce() -> Result<Option<Block>, AdmissionFailure>,
        retain: impl FnOnce(u64) -> Result<Option<ByteLease>, AdmissionFailure>,
    ) -> Result<Option<OwnedBlock>, AdmissionFailure> {
        if !matches!(self.0.limit, RetentionLimit::Blocks(_)) {
            return Err(AdmissionFailure::DecodeRequiresCountLimit);
        }
        let mut charge = self.reserve(0, Admission::WithinLimit)?;
        let Some(mut block) = decode()? else {
            return Ok(None);
        };
        if Arc::get_mut(&mut block.header).is_none()
            || block
                .transactions
                .iter_mut()
                .any(|transaction| Arc::get_mut(transaction).is_none())
        {
            return Err(AdmissionFailure::AliasedPayload);
        }
        let bytes = block.attributed_memory_size_bytes();
        let retained = retain(bytes)?;
        if self
            .0
            .counted_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes)
            })
            .is_err()
        {
            self.close();
            return Err(AdmissionFailure::Closed);
        }
        charge.bytes = bytes;
        if self.is_closed() {
            return Err(AdmissionFailure::Closed);
        }
        Ok(Some(OwnedBlock(Arc::new(RetainedBlock {
            block,
            _retained: retained,
            charge: Mutex::new(charge),
            bytes,
        }))))
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
            _retained: None,
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
    _retained: Option<ByteLease>,
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
    /// A separately decoded header that retains its own admission.
    Standalone(OwnedHeader),
}

impl Deref for HeaderHandle {
    type Target = Header;

    fn deref(&self) -> &Header {
        match self {
            Self::Shared(header) => header,
            Self::Block(block) => &block.0.block.header,
            Self::Standalone(header) => header,
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
    /// A separately decoded transaction that retains its own admission.
    Standalone(OwnedTransaction),
}

impl Deref for TransactionHandle {
    type Target = Transaction;

    fn deref(&self) -> &Transaction {
        match self {
            Self::Shared(transaction) => transaction,
            Self::Owned(transaction) => transaction,
            Self::Standalone(transaction) => transaction,
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

#[derive(Debug)]
struct RetainedPayload<T> {
    // Destroy the actual payload before releasing its admission.
    payload: T,
    _retained: Option<ByteLease>,
    _permit: OwnedSemaphorePermit,
}

/// A standalone payload whose final holder releases its read admission.
/// The owner exposes borrowed data, never a raw shared handle.
#[derive(Debug)]
pub struct OwnedPayload<T>(Arc<RetainedPayload<T>>);

impl<T> Clone for OwnedPayload<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

/// A standalone transaction decoded from disk.
pub type OwnedTransaction = OwnedPayload<Transaction>;

/// A standalone header decoded from disk.
pub type OwnedHeader = OwnedPayload<Header>;

/// One block's serialized bytes copied from disk.
pub type OwnedRawBlock = OwnedPayload<Vec<u8>>;

/// Shared count admission for one kind of standalone read result.
#[derive(Clone, Debug)]
pub struct DecodedPayloadPool(Arc<Semaphore>);

/// Why a standalone payload cannot acquire read admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PayloadAdmissionFailure {
    /// Other readers or retained results hold every slot.
    Capacity,
    /// This pool generation has permanently closed admission.
    Closed,
}

impl fmt::Display for PayloadAdmissionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Capacity => "decoded payload capacity exhausted",
            Self::Closed => "decoded payload pool closed",
        })
    }
}

impl std::error::Error for PayloadAdmissionFailure {}

impl From<ReadMemoryExhausted> for PayloadAdmissionFailure {
    fn from(_: ReadMemoryExhausted) -> Self {
        Self::Capacity
    }
}

impl DecodedPayloadPool {
    /// Create one count allowance shared by this pool's clones.
    pub fn new(limit: NonZeroU16) -> Self {
        Self(Arc::new(Semaphore::new(usize::from(limit.get()))))
    }

    /// Read the actual semaphore capacity without waiting for a lock or worker.
    pub fn available_slots(&self) -> usize {
        self.0.available_permits()
    }

    /// Acquire a count slot before lookup or decoding. Missing data and unwinding
    /// release the slot. Use this only for payloads with a fixed maximum size, where
    /// the count bounds their bytes. [`Self::try_decode_bounded`] covers other payloads.
    pub fn try_decode<T>(
        &self,
        decode: impl FnOnce() -> Option<T>,
    ) -> Result<Option<OwnedPayload<T>>, PayloadAdmissionFailure> {
        let permit = self.acquire()?;
        Ok(decode().map(|payload| {
            OwnedPayload(Arc::new(RetainedPayload {
                payload,
                _retained: None,
                _permit: permit,
            }))
        }))
    }

    /// Decode like [`Self::try_decode`], and also charge `memory`.
    ///
    /// The steps run in this order: acquire the slot, parse under `decode`'s scope,
    /// measure the payload, fund its retained bytes, and release the parser charge.
    /// Refusal and unwinding release every earlier step.
    pub fn try_decode_bounded<T: DecodedSize>(
        &self,
        memory: &ReadMemoryBudget,
        decode: impl FnOnce(&mut ParseScope) -> Result<Option<T>, ReadMemoryExhausted>,
    ) -> Result<Option<OwnedPayload<T>>, PayloadAdmissionFailure> {
        let permit = self.acquire()?;
        let mut parsing = memory.parse_scope();
        let Some(payload) = decode(&mut parsing)? else {
            return Ok(None);
        };
        let retained = memory.retain(payload.decoded_size_bytes())?;
        drop(parsing);
        Ok(Some(OwnedPayload(Arc::new(RetainedPayload {
            payload,
            _retained: Some(retained),
            _permit: permit,
        }))))
    }

    fn acquire(&self) -> Result<OwnedSemaphorePermit, PayloadAdmissionFailure> {
        self.0
            .clone()
            .try_acquire_owned()
            .map_err(|error| match error {
                tokio::sync::TryAcquireError::NoPermits => PayloadAdmissionFailure::Capacity,
                tokio::sync::TryAcquireError::Closed => PayloadAdmissionFailure::Closed,
            })
    }
}

/// Reports the memory a decoded read result retains, including its inline size.
pub trait DecodedSize {
    /// Attributed bytes retained by this payload.
    fn decoded_size_bytes(&self) -> u64;
}

impl DecodedSize for Transaction {
    fn decoded_size_bytes(&self) -> u64 {
        self.decoded_memory_size_bytes()
    }
}

impl DecodedSize for Vec<u8> {
    fn decoded_size_bytes(&self) -> u64 {
        u64::try_from(self.capacity()).unwrap_or(u64::MAX)
    }
}

/// A read refused because other holders use the rest of a memory limit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReadMemoryExhausted;

impl fmt::Display for ReadMemoryExhausted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("read memory limit exhausted")
    }
}

impl std::error::Error for ReadMemoryExhausted {}

#[derive(Debug)]
struct ByteGauge {
    limit: u64,
    used: AtomicU64,
}

impl ByteGauge {
    fn new(limit: NonZeroU64) -> Arc<Self> {
        Arc::new(Self {
            limit: limit.get(),
            used: AtomicU64::new(0),
        })
    }

    /// Add `bytes` for a holder that already owns `held`. The gauge refuses growth
    /// past its limit only while another holder exists, so a lone reader finishes.
    fn try_add(&self, held: u64, bytes: u64) -> Result<(), ReadMemoryExhausted> {
        self.used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                let next = used.checked_add(bytes)?;
                (next <= self.limit || used == held).then_some(next)
            })
            .map(drop)
            .map_err(|_| ReadMemoryExhausted)
    }

    fn release(&self, bytes: u64) {
        let _ = self
            .used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                Some(used.saturating_sub(bytes))
            });
    }

    fn used(&self) -> u64 {
        self.used.load(Ordering::Acquire)
    }
}

/// Bytes charged to one gauge until this lease drops.
#[derive(Debug)]
struct ByteLease {
    gauge: Arc<ByteGauge>,
    bytes: u64,
}

impl ByteLease {
    fn try_new(gauge: &Arc<ByteGauge>, bytes: u64) -> Result<Self, ReadMemoryExhausted> {
        gauge.try_add(0, bytes)?;
        Ok(Self {
            gauge: gauge.clone(),
            bytes,
        })
    }
}

impl Drop for ByteLease {
    fn drop(&mut self) {
        self.gauge.release(self.bytes);
    }
}

/// Separate memory limits for parser transients and retained read results.
///
/// A parser charge covers raw rows, partial results, and output buffers during one
/// read. A retained charge covers one measured result until its final holder drops.
/// A result funds its retained charge before it releases its parser charge, so it
/// never leaves both gauges during the handoff. Clones share both gauges.
#[derive(Clone, Debug)]
pub struct ReadMemoryBudget {
    parser: Arc<ByteGauge>,
    retained: Arc<ByteGauge>,
}

impl ReadMemoryBudget {
    /// Create one budget. Callers check that the limits fund a legal payload.
    pub fn new(parser_limit: NonZeroU64, retained_limit: NonZeroU64) -> Self {
        Self {
            parser: ByteGauge::new(parser_limit),
            retained: ByteGauge::new(retained_limit),
        }
    }

    /// Bytes currently charged to active parsers.
    pub fn parser_bytes(&self) -> u64 {
        self.parser.used()
    }

    /// Bytes currently charged to retained results.
    pub fn retained_bytes(&self) -> u64 {
        self.retained.used()
    }

    fn parse_scope(&self) -> ParseScope {
        ParseScope(Some(ByteLease {
            gauge: self.parser.clone(),
            bytes: 0,
        }))
    }

    fn retain(&self, bytes: u64) -> Result<ByteLease, ReadMemoryExhausted> {
        ByteLease::try_new(&self.retained, bytes)
    }
}

/// Parser memory charged by one read. Dropping the scope releases its whole charge.
#[derive(Debug)]
pub struct ParseScope(Option<ByteLease>);

impl ParseScope {
    /// Create a scope for a read whose owner proves its parser bound statically,
    /// such as a fixed worker partition multiplied by one block's size.
    pub fn statically_bounded() -> Self {
        Self(None)
    }

    /// Charge memory before or after allocating it, then keep it through this read.
    pub fn try_charge(&mut self, bytes: u64) -> Result<(), ReadMemoryExhausted> {
        if let Some(lease) = &mut self.0 {
            lease.gauge.try_add(lease.bytes, bytes)?;
            lease.bytes = lease.bytes.saturating_add(bytes);
        }
        Ok(())
    }

    /// Return memory this read already freed.
    pub fn release(&mut self, bytes: u64) {
        if let Some(lease) = &mut self.0 {
            let bytes = bytes.min(lease.bytes);
            lease.gauge.release(bytes);
            lease.bytes -= bytes;
        }
    }
}

impl<T: PartialEq> PartialEq for OwnedPayload<T> {
    fn eq(&self, other: &Self) -> bool {
        self.0.payload == other.0.payload
    }
}

impl<T: Eq> Eq for OwnedPayload<T> {}

impl<T> Deref for OwnedPayload<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.0.payload
    }
}

impl From<OwnedTransaction> for TransactionHandle {
    fn from(transaction: OwnedTransaction) -> Self {
        Self::Standalone(transaction)
    }
}

impl From<OwnedHeader> for HeaderHandle {
    fn from(header: OwnedHeader) -> Self {
        Self::Standalone(header)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zakura_chain::serialization::ZcashDeserialize;

    fn block() -> Block {
        Block::zcash_deserialize(&zakura_test::vectors::BLOCK_MAINNET_1_BYTES[..]).unwrap()
    }

    fn memory(parser: u64, retained: u64) -> ReadMemoryBudget {
        ReadMemoryBudget::new(
            NonZeroU64::new(parser).unwrap(),
            NonZeroU64::new(retained).unwrap(),
        )
    }

    fn ten_blocks() -> DecodedBlockPool {
        DecodedBlockPool::for_block_count(NonZeroU64::new(10).unwrap())
    }

    #[test]
    fn parser_charge_moves_to_a_separate_retained_charge_at_the_handoff() {
        let bytes = block().attributed_memory_size_bytes();
        let budget = memory(4096, 2 * bytes);
        let observed = budget.clone();
        let owner = ten_blocks()
            .try_decode_bounded(&budget, |scope| {
                scope.try_charge(4096)?;
                assert_eq!(observed.parser_bytes(), 4096);
                assert_eq!(observed.retained_bytes(), 0);
                Ok(Some(block()))
            })
            .unwrap()
            .unwrap();
        assert_eq!(budget.parser_bytes(), 0, "the result left its parser");
        assert_eq!(budget.retained_bytes(), bytes);

        let transaction = owner.transaction(0).unwrap();
        drop(owner);
        assert_eq!(budget.retained_bytes(), bytes, "a clone keeps the charge");
        drop(transaction);
        assert_eq!(budget.retained_bytes(), 0);
    }

    #[test]
    fn exhausted_parser_limit_refuses_without_touching_retained_capacity() {
        let budget = memory(100, u64::MAX);
        let pool = ten_blocks();
        let mut outer = budget.parse_scope();
        outer.try_charge(100).unwrap();
        let error = pool
            .try_decode_bounded(&budget, |scope| {
                scope.try_charge(1)?;
                unreachable!("the parser limit refuses this read")
            })
            .unwrap_err();
        assert_eq!(error, AdmissionFailure::Capacity);
        assert_eq!(budget.parser_bytes(), 100, "only the other reader remains");
        assert_eq!(budget.retained_bytes(), 0);
        assert_eq!(pool.reserved_blocks(), Some(0), "the count rolled back");
    }

    #[test]
    fn exhausted_retained_limit_rolls_back_count_and_parser_charges() {
        let bytes = block().attributed_memory_size_bytes();
        let budget = memory(u64::MAX, bytes);
        let pool = ten_blocks();
        let first = pool
            .try_decode_bounded(&budget, |_| Ok(Some(block())))
            .unwrap()
            .unwrap();
        let error = pool
            .try_decode_bounded(&budget, |scope| {
                scope.try_charge(7)?;
                Ok(Some(block()))
            })
            .unwrap_err();
        assert_eq!(error, AdmissionFailure::Capacity);
        assert_eq!(budget.parser_bytes(), 0);
        assert_eq!(budget.retained_bytes(), bytes);
        assert_eq!(pool.reserved_blocks(), Some(1));
        assert_eq!(pool.reserved_bytes(), bytes);

        drop(first);
        assert!(pool
            .try_decode_bounded(&budget, |_| Ok(Some(block())))
            .unwrap()
            .is_some());
    }

    #[test]
    fn unwinding_parser_releases_every_reservation() {
        let budget = memory(1 << 20, 1 << 20);
        let pool = ten_blocks();
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = pool.try_decode_bounded(&budget, |scope| {
                scope.try_charge(1024)?;
                panic!("interrupted disk parser")
            });
        }));
        assert!(panic.is_err());
        assert_eq!(budget.parser_bytes(), 0);
        assert_eq!(budget.retained_bytes(), 0);
        assert_eq!(pool.reserved_blocks(), Some(0));
    }

    #[test]
    fn lone_reader_progresses_past_each_limit_but_a_second_holder_is_refused() {
        let bytes = block().attributed_memory_size_bytes();
        let budget = memory(1, 1);
        let pool = ten_blocks();
        let oversized = pool
            .try_decode_bounded(&budget, |scope| {
                scope.try_charge(1024)?;
                scope.try_charge(1024)?;
                Ok(Some(block()))
            })
            .unwrap()
            .unwrap();
        assert_eq!(budget.retained_bytes(), bytes);
        assert_eq!(
            pool.try_decode_bounded(&budget, |_| Ok(Some(block())))
                .unwrap_err(),
            AdmissionFailure::Capacity,
        );
        drop(oversized);
        assert_eq!(budget.retained_bytes(), 0);
    }

    #[test]
    fn parser_release_returns_freed_rows_and_ignores_excess() {
        let budget = memory(100, 100);
        let mut scope = budget.parse_scope();
        scope.try_charge(80).unwrap();
        scope.release(30);
        assert_eq!(budget.parser_bytes(), 50);
        scope.try_charge(50).unwrap();
        scope.release(1000);
        assert_eq!(budget.parser_bytes(), 0);
        drop(scope);
        assert_eq!(budget.parser_bytes(), 0);
    }

    #[test]
    fn static_scope_charges_no_gauge() {
        let mut scope = ParseScope::statically_bounded();
        scope.try_charge(u64::MAX).unwrap();
        scope.release(u64::MAX);
    }

    #[test]
    fn standalone_payload_retains_measured_bytes_through_its_final_clone() {
        let budget = memory(1 << 20, 64);
        let pool = DecodedPayloadPool::new(NonZeroU16::new(4).unwrap());
        let payload = pool
            .try_decode_bounded(&budget, |scope| {
                scope.try_charge(48)?;
                assert_eq!(budget.parser_bytes(), 48);
                Ok(Some(Vec::<u8>::with_capacity(48)))
            })
            .unwrap()
            .unwrap();
        let measured = payload.decoded_size_bytes();
        assert_eq!(budget.parser_bytes(), 0);
        assert_eq!(budget.retained_bytes(), measured);
        let clone = payload.clone();
        drop(payload);
        assert_eq!(budget.retained_bytes(), measured);

        let second = budget.retain(1).unwrap();
        let refused = pool.try_decode_bounded(&budget, |_| Ok(Some(vec![0u8; 64])));
        assert_eq!(refused.unwrap_err(), PayloadAdmissionFailure::Capacity);
        assert_eq!(
            pool.available_slots(),
            3,
            "the refused read gave back its slot"
        );
        drop((clone, second));
        assert_eq!(budget.retained_bytes(), 0);
        assert_eq!(pool.available_slots(), 4);
    }

    #[test]
    fn decoded_size_counts_inline_and_nested_transaction_memory() {
        let block = block();
        let transaction = &block.transactions[0];
        assert!(
            transaction.decoded_size_bytes()
                > u64::try_from(std::mem::size_of::<Transaction>()).unwrap()
        );
    }

    #[test]
    fn decode_admission_precedes_materialization_and_survives_final_holders() {
        let pool = DecodedBlockPool::for_block_count(NonZeroU64::MIN);
        let owner = pool
            .try_decode(|| {
                assert_eq!(pool.reserved_blocks(), Some(1));
                Some(block())
            })
            .unwrap()
            .unwrap();
        let actual = Arc::downgrade(&owner.0.block.header);
        let transaction = owner.transaction(0).unwrap();
        drop(owner);
        assert!(actual.upgrade().is_some());
        let called = std::cell::Cell::new(false);
        let error = pool
            .try_decode(|| {
                called.set(true);
                Some(block())
            })
            .unwrap_err();
        assert_eq!(error, AdmissionFailure::Capacity);
        assert!(!called.get(), "refusal precedes disk reads and decoding");
        drop(transaction);
        assert!(actual.upgrade().is_none());
        assert_eq!(pool.reserved_blocks(), Some(0));
        assert_eq!(pool.reserved_bytes(), 0);
    }

    #[test]
    fn absent_or_panicking_decode_settles_its_reservation() {
        let pool = DecodedBlockPool::for_block_count(NonZeroU64::MIN);
        assert!(pool.try_decode(|| None).unwrap().is_none());
        assert_eq!(pool.reserved_blocks(), Some(0));
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = pool.try_decode(|| {
                let _block = block();
                assert_eq!(pool.reserved_blocks(), Some(1));
                panic!("interrupted disk decode")
            });
        }));
        assert!(panic.is_err());
        assert_eq!(pool.reserved_blocks(), Some(0));
        assert_eq!(pool.reserved_bytes(), 0);
        assert!(pool.try_decode(|| Some(block())).unwrap().is_some());
    }

    #[test]
    fn standalone_transaction_admission_precedes_decode_and_outlives_clones() {
        let pool = DecodedPayloadPool::new(NonZeroU16::MIN);
        let transaction = pool
            .try_decode(|| {
                assert_eq!(pool.available_slots(), 0, "admission precedes decode");
                Some((*block().transactions[0]).clone())
            })
            .unwrap()
            .unwrap();
        let probe = model::PayloadProbe::new(&transaction);
        let clone = transaction.clone();
        drop(transaction);
        assert_eq!(probe.holders(), 1);
        assert!(probe.serialized_bytes().unwrap() > 0);
        assert_eq!(
            pool.available_slots(),
            0,
            "live standalone allocation retains its permit"
        );
        let error = pool
            .try_decode::<Transaction>(|| panic!("refused decode ran"))
            .unwrap_err();
        assert_eq!(error, PayloadAdmissionFailure::Capacity);
        drop(clone);
        assert_eq!(probe.holders(), 0);
        assert_eq!(probe.serialized_bytes(), None);
        assert_eq!(pool.available_slots(), 1);
    }

    #[test]
    fn header_slots_bound_retained_header_bytes() {
        // A header has no heap-backed fields, so a slot bounds its whole allocation.
        assert!(std::mem::size_of::<RetainedPayload<Header>>() <= 2048);
    }

    #[test]
    fn standalone_transaction_missing_panic_and_closed_pool_settle_admission() {
        let pool = DecodedPayloadPool::new(NonZeroU16::MIN);
        assert!(pool.try_decode::<Transaction>(|| None).unwrap().is_none());
        assert_eq!(pool.available_slots(), 1);
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = pool.try_decode::<Transaction>(|| panic!("interrupted decode"));
        }));
        assert!(panic.is_err());
        assert_eq!(pool.available_slots(), 1);
        pool.0.close();
        assert_eq!(
            pool.try_decode::<Transaction>(|| panic!("closed decode ran"))
                .unwrap_err(),
            PayloadAdmissionFailure::Closed
        );
    }

    #[test]
    fn equal_transactions_settle_only_their_original_pool() {
        let first = DecodedPayloadPool::new(NonZeroU16::MIN);
        let second = DecodedPayloadPool::new(NonZeroU16::MIN);
        let decode = || Some((*block().transactions[0]).clone());
        let a = first.try_decode(decode).unwrap().unwrap();
        let b = second.try_decode(decode).unwrap().unwrap();
        assert_eq!(a.hash(), b.hash());
        let probe = model::PayloadProbe::new(&b);
        drop(a);
        assert_eq!(first.available_slots(), 1);
        assert_eq!(second.available_slots(), 0);
        assert_eq!(probe.holders(), 1);
        drop(b);
        assert_eq!(second.available_slots(), 1);
        assert_eq!(probe.holders(), 0);
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
