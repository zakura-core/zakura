//! Decoded payload retention across ingress, verification, and state storage.
//!
//! Block ingress reserves parser transients separately from retained admission.
//! Block and derived transaction handles share one allocation and charge.
//! A charge never changes pool, so its last holder settles the pool that admitted it.
//! Disk reads reserve retained bytes in one [`ReadMemoryBudget`] before
//! materialization, then settle to the measured result. Every result keeps its
//! charge through its final holder.
//!
//! # Capacity and progress contract
//!
//! Every admission in this module refuses instead of waiting, so no caller can
//! wait for capacity while it holds a lock, a worker slot, or an earlier reservation.
//!
//! | Actor | Contract |
//! | --- | --- |
//! | Waiting caller | Waits for a worker slot before it holds any lock or memory charge. Dropping the wait releases the request. |
//! | Reader | Reserves an upper bound on its result's bytes, parses inside its worker slot, then settles the reservation to the measured result. A refusal at any step releases every earlier step. |
//! | Snapshot | Holds its retained handles for one read job. The job's end releases them, so a snapshot cannot hold capacity for an idle peer. |
//! | Fork | A block handle keeps its charge after its chain leaves the non-finalized state. Its last holder settles the charge. |
//! | Writer | Takes no ordinary result capacity. Chain-tip publication uses a fixed partition, so readers cannot stall a commit. |
//! | Rayon work | Holds no shared lock across a wait for its pool. A detached verifier job owns its payload handle until the job drops it. |
//!
//! A reservation always succeeds when its budget has no other holder, even above the
//! limit. Refusal therefore proves that another holder exists and that its release
//! allows a retry. This bounds the budget by its limit plus one legal payload.
//!
//! Disk retention has no representation here. Store policies own durable limits,
//! and a memory lease says nothing about what a commit wrote.
//!
//! # Decoded limits
//!
//! Three limits bound decoded blocks in block sync, and each has one enforcer.
//!
//! | Limit | Bounds | Enforcer |
//! | --- | --- | --- |
//! | Handoff cap, `BS_MAX_DECODED_SUBMISSION_WINDOW_BYTES` | Decoded bytes in the sequencer input queue and `applying`. | `charge_decoded_handoff`, before admission. Refusal hands over raw bytes. |
//! | Pipeline bound, `decoded_pipeline_byte_bound` | Decoded bytes the pipeline still holds. | This pool's [`Admission::WithinLimit`]. [`Admission::PipelineExempt`] passes it for the commit window. |
//! | Count allowance, [`DecodedBlockPool::for_block_count`] and [`DecodedBlockPool::for_pipeline`] | Allocations with any holder, including holders past the pipeline. | This pool, for every admission. |
//!
//! A block leaves the pipeline bound when its pipeline work ends, so downstream
//! retention can never refuse the work that would let downstream stores release
//! their blocks.

use std::{fmt, io, num::NonZeroU64, ops::Deref, sync::Arc};

use zakura_chain::{
    block::{Block, BlockRef, Header},
    serialization::ZcashSerialize,
    transaction::Transaction,
};
use zakura_rt::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Notify,
};

/// Updates an `AtomicU64` with `AcqRel` and `Acquire` orderings, keeping the
/// value when `f` returns `None`. Rust 1.99 deprecates std's `fetch_update` in
/// favor of `try_update`, which the 1.97 MSRV lacks. The simulator's atomic has
/// its own `fetch_update`, so its schedule stays the same.
pub(crate) trait CheckedUpdate {
    /// Applies `f` atomically; returns the previous value, or the current one on refusal.
    fn checked_update(&self, f: impl FnMut(u64) -> Option<u64>) -> Result<u64, u64>;
}

impl CheckedUpdate for AtomicU64 {
    #[allow(deprecated)]
    fn checked_update(&self, f: impl FnMut(u64) -> Option<u64>) -> Result<u64, u64> {
        self.fetch_update(Ordering::AcqRel, Ordering::Acquire, f)
    }
}

#[cfg(any(test, zakura_sim, feature = "test-support"))]
pub mod model;

/// Capacity for decoded blocks, including references retained outside their store.
#[derive(Clone, Debug)]
pub struct DecodedBlockPool(Arc<Pool>);

#[derive(Debug)]
struct Pool {
    /// The most allocations this pool retains.
    block_limit: u64,
    /// Allocations this pool currently retains.
    reserved: AtomicU64,
    /// Attributed decoded bytes of those allocations.
    counted_bytes: AtomicU64,
    capacity_version: AtomicU64,
    closed: AtomicBool,
    capacity: Notify,
    pipeline_limit: Option<u64>,
    pipeline_bytes: AtomicU64,
}

/// Admission rule selected by the owning pipeline.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Admission {
    /// Admit only within the pool's configured limit.
    WithinLimit,
    /// Pass the pipeline byte limit and keep every other limit.
    /// The caller's own count window bounds these blocks, and they stay charged.
    PipelineExempt,
}

/// Why a decoded block cannot enter a pool.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmissionFailure {
    /// A header or transaction has a holder outside the proposed owner.
    AliasedPayload,
    /// Existing holders or the block's size leave insufficient capacity.
    Capacity,
    /// This pool generation has permanently closed admission.
    Closed,
    /// Pre-decode admission requires a pool without a pipeline byte limit.
    DecodeRequiresCountLimit,
}

impl fmt::Display for AdmissionFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::AliasedPayload => "decoded block contains shared payloads",
            Self::Capacity => "decoded block capacity exhausted",
            Self::Closed => "decoded block pool closed",
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

/// A decoded block and its attributed size, measured once when the block is
/// wrapped. Only [`MeasuredBlock::new`] builds one, so the size always
/// matches the block.
#[derive(Debug)]
pub struct MeasuredBlock {
    block: Block,
    bytes: u64,
}

impl MeasuredBlock {
    /// Measure `block`'s attributed size.
    pub fn new(block: Block) -> Self {
        let bytes = block.attributed_memory_size_bytes();
        Self { block, bytes }
    }

    /// The attributed size measured when the block was wrapped.
    pub fn bytes(&self) -> u64 {
        self.bytes
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
    /// Enforce a block-count allowance while tracking its actual decoded bytes.
    /// Checkpoint and backend owners can preserve their existing count contracts.
    /// This constructor does not establish a decoded-byte bound.
    pub fn for_block_count(limit: NonZeroU64) -> Self {
        Self::with_limit(limit, None)
    }

    /// Reserve one block before reading or decoding it, then retain that reservation
    /// through its final holder. Missing data and unwinding release the reservation.
    ///
    /// Only count-limited pools support this operation. The count includes active
    /// decoders and returned blocks. Decoded bytes are measured after materialization.
    /// Use this only where a fixed count is the memory bound, such as a small partition.
    pub fn try_decode(
        &self,
        decode: impl FnOnce() -> Option<Block>,
    ) -> Result<Option<OwnedBlock>, AdmissionFailure> {
        self.decode_admitted(None, decode)
    }

    /// Decode like [`Self::try_decode`] under a byte reservation the caller took before
    /// reading. The reservation settles to the block's measured bytes and follows the
    /// block to its final holder. Refusal and unwinding release every earlier step.
    pub fn try_decode_reserved(
        &self,
        reservation: ReadReservation,
        decode: impl FnOnce() -> Option<Block>,
    ) -> Result<Option<OwnedBlock>, AdmissionFailure> {
        self.decode_admitted(Some(reservation), decode)
    }

    fn decode_admitted(
        &self,
        reservation: Option<ReadReservation>,
        decode: impl FnOnce() -> Option<Block>,
    ) -> Result<Option<OwnedBlock>, AdmissionFailure> {
        // A pipeline pool charges measured bytes at admission, and a decode reserves
        // before it can measure them.
        if self.0.pipeline_limit.is_some() {
            return Err(AdmissionFailure::DecodeRequiresCountLimit);
        }
        let mut charge = self.reserve(0, Admission::WithinLimit)?;
        let Some(mut block) = decode() else {
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
        let retained = reservation
            .map(|reservation| reservation.0.settle(bytes))
            .transpose()?;
        if self
            .0
            .counted_bytes
            .checked_update(|used| used.checked_add(bytes))
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
            bytes,
            charge,
        }))))
    }

    /// Enforce a block-count allowance on all holders and a byte limit on the
    /// bytes their pipeline holds. [`OwnedBlock::leave_pipeline`] ends a block's
    /// byte share while downstream holders keep its count and retained bytes.
    pub fn for_pipeline(blocks: NonZeroU64, pipeline_bytes: u64) -> Self {
        Self::with_limit(blocks, Some(pipeline_bytes))
    }

    fn with_limit(block_limit: NonZeroU64, pipeline_limit: Option<u64>) -> Self {
        Self(Arc::new(Pool {
            block_limit: block_limit.get(),
            reserved: AtomicU64::new(0),
            counted_bytes: AtomicU64::new(0),
            capacity_version: AtomicU64::new(0),
            closed: AtomicBool::new(false),
            capacity: Notify::new(),
            pipeline_limit,
            pipeline_bytes: AtomicU64::new(0),
        }))
    }

    /// Admit an exclusively held block before allocating its shared owner.
    /// Existing strong or weak header/transaction aliases prevent adoption.
    pub fn try_admit(
        &self,
        block: Block,
        admission: Admission,
    ) -> Result<OwnedBlock, RejectedBlock> {
        self.try_admit_measured(MeasuredBlock::new(block), admission)
    }

    /// [`Self::try_admit`] for a block whose caller already measured it.
    pub fn try_admit_measured(
        &self,
        block: MeasuredBlock,
        admission: Admission,
    ) -> Result<OwnedBlock, RejectedBlock> {
        let MeasuredBlock { mut block, bytes } = block;
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
            bytes,
            charge,
        })))
    }

    /// Current attributed decoded bytes, including detached holders.
    pub fn reserved_bytes(&self) -> u64 {
        self.0.counted_bytes.load(Ordering::Acquire)
    }

    /// Attributed decoded bytes the pipeline still holds. Zero without a pipeline limit.
    pub fn pipeline_bytes(&self) -> u64 {
        self.0.pipeline_bytes.load(Ordering::Acquire)
    }

    /// Current retained allocations.
    pub fn reserved_blocks(&self) -> u64 {
        self.0.reserved.load(Ordering::Acquire)
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
            .checked_update(|version| version.checked_add(1))
            .is_err()
        {
            self.0.closed.store(true, Ordering::Release);
        }
        self.0.capacity.notify_waiters();
    }

    /// Whether the current holders leave room to admit a block of `bytes` under
    /// `admission`, closure aside. Admission checks again, so a stale `true`
    /// costs one attempt. Read [`Self::capacity_version`] first, so a release
    /// after a stale `false` still wakes the caller.
    pub fn has_room_for(&self, bytes: u64, admission: Admission) -> bool {
        if self.0.reserved.load(Ordering::Acquire) >= self.0.block_limit {
            return false;
        }
        match self.0.pipeline_limit {
            Some(limit) if admission == Admission::WithinLimit => self
                .0
                .pipeline_bytes
                .load(Ordering::Acquire)
                .checked_add(bytes)
                .is_some_and(|next| next <= limit),
            _ => true,
        }
    }

    /// Register a notification before retrying admission, then await it on refusal.
    pub fn capacity(&self) -> &Notify {
        &self.0.capacity
    }

    fn reserve(&self, bytes: u64, admission: Admission) -> Result<Charge, AdmissionFailure> {
        if self.is_closed() {
            return Err(AdmissionFailure::Closed);
        }
        let pipeline_bytes = self.charge_pipeline(bytes, admission)?;
        match self.reserve_retention(bytes) {
            Ok(charge) => {
                charge
                    .pipeline_bytes
                    .store(pipeline_bytes, Ordering::Release);
                Ok(charge)
            }
            Err(reason) => {
                if pipeline_bytes > 0 {
                    // Another caller may have been refused because of this brief charge.
                    self.release_pipeline(pipeline_bytes);
                    self.notify_capacity();
                }
                Err(reason)
            }
        }
    }

    /// Take the block's pipeline share and return the bytes charged.
    /// A count refusal returns first, so it causes no rollback wakeups.
    fn charge_pipeline(&self, bytes: u64, admission: Admission) -> Result<u64, AdmissionFailure> {
        let Some(limit) = self.0.pipeline_limit else {
            return Ok(0);
        };
        if self.0.reserved.load(Ordering::Acquire) >= self.0.block_limit {
            return Err(AdmissionFailure::Capacity);
        }
        self.0
            .pipeline_bytes
            .checked_update(|used| {
                let next = used.checked_add(bytes)?;
                (next <= limit || admission == Admission::PipelineExempt).then_some(next)
            })
            .map_err(|_| AdmissionFailure::Capacity)?;
        Ok(bytes)
    }

    fn release_pipeline(&self, bytes: u64) {
        if self
            .0
            .pipeline_bytes
            .checked_update(|used| used.checked_sub(bytes))
            .is_err()
        {
            self.close();
        }
    }

    fn reserve_retention(&self, bytes: u64) -> Result<Charge, AdmissionFailure> {
        let mut used = self.0.reserved.load(Ordering::Acquire);
        loop {
            if self.is_closed() {
                return Err(AdmissionFailure::Closed);
            }
            let Some(next) = used.checked_add(1) else {
                return Err(AdmissionFailure::Capacity);
            };
            if next > self.0.block_limit {
                return Err(AdmissionFailure::Capacity);
            }
            match self.0.reserved.compare_exchange_weak(
                used,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    if self
                        .0
                        .counted_bytes
                        .checked_update(|used| used.checked_add(bytes))
                        .is_err()
                    {
                        self.close();
                        let _ = self.0.reserved.checked_update(|used| used.checked_sub(1));
                        return Err(AdmissionFailure::Closed);
                    }
                    let charge = Charge {
                        pool: self.clone(),
                        bytes,
                        pipeline_bytes: AtomicU64::new(0),
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
    pipeline_bytes: AtomicU64,
}

impl Charge {
    fn leave_pipeline(&self) {
        let bytes = self.pipeline_bytes.swap(0, Ordering::AcqRel);
        if bytes > 0 {
            self.pool.release_pipeline(bytes);
            self.pool.notify_capacity();
        }
    }
}

impl Drop for Charge {
    fn drop(&mut self) {
        let pipeline_bytes = *self.pipeline_bytes.get_mut();
        if pipeline_bytes > 0 {
            self.pool.release_pipeline(pipeline_bytes);
        }
        if self
            .pool
            .0
            .counted_bytes
            .checked_update(|used| used.checked_sub(self.bytes))
            .is_err()
        {
            self.pool.close();
            return;
        }
        if self
            .pool
            .0
            .reserved
            .checked_update(|used| used.checked_sub(1))
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
    bytes: u64,
    charge: Charge,
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
/// use zakura_chain::transaction::Transaction;
/// use zakura_node_services::block_memory::OwnedBlock;
/// fn escape(owner: &OwnedBlock) -> Arc<Transaction> {
///     owner.transactions().next().unwrap().clone()
/// }
/// ```
#[derive(Clone, Debug)]
pub struct OwnedBlock(Arc<RetainedBlock>);

impl Deref for OwnedBlock {
    type Target = BlockRef;

    fn deref(&self) -> &BlockRef {
        &self.0.block
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
    pub fn transaction(&self, index: usize) -> Option<TransactionHandle> {
        self.0.block.transactions.get(index)?;
        Some(TransactionHandle::Block {
            block: self.clone(),
            index,
        })
    }

    /// End this block's pipeline byte share. Downstream handles keep its retained
    /// charge. Repeating the call changes nothing.
    pub fn leave_pipeline(&self) {
        self.0.charge.leave_pipeline();
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

    /// Attributed decoded bytes of this block. A shared block measures them on each call.
    pub fn decoded_bytes(&self) -> u64 {
        match self {
            Self::Shared(block) => block.attributed_memory_size_bytes(),
            Self::Owned(block) => block.decoded_bytes(),
        }
    }

    /// End the pipeline byte share when this handle owns a charge.
    pub fn leave_pipeline(&self) {
        if let Self::Owned(block) = self {
            block.leave_pipeline();
        }
    }

    /// Borrow payloads without exposing raw shared handles.
    /// Callers use it to reach `BlockRef::transactions`, which [`Self::transactions`]
    /// shadows, and `BlockRef::header` where an extension trait's `header` shadows it.
    pub fn view(&self) -> &BlockRef {
        self.block()
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
            Self::Owned(block) => block.transaction(index),
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
}

impl Deref for BlockHandle {
    type Target = BlockRef;

    fn deref(&self) -> &BlockRef {
        self.block()
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
    /// [`OwnedBlock::transaction`] checks `index`, and the block is immutable.
    Block {
        /// The block whose charge this handle retains.
        block: OwnedBlock,
        /// The transaction's position in `block`.
        index: usize,
    },
    /// A separately decoded transaction that retains its own admission.
    Standalone(OwnedTransaction),
}

impl Deref for TransactionHandle {
    type Target = Transaction;

    fn deref(&self) -> &Transaction {
        match self {
            Self::Shared(transaction) => transaction,
            Self::Block { block, index } => &block.0.block.transactions[*index],
            Self::Standalone(transaction) => transaction,
        }
    }
}

/// The comparison and conversion impls of a handle that derefs to its payload.
macro_rules! payload_handle {
    ($handle:ident, $payload:ty) => {
        impl AsRef<$payload> for $handle {
            fn as_ref(&self) -> &$payload {
                self
            }
        }

        impl std::borrow::Borrow<$payload> for $handle {
            fn borrow(&self) -> &$payload {
                self
            }
        }

        impl PartialEq for $handle {
            fn eq(&self, other: &Self) -> bool {
                self.as_ref() == other.as_ref()
            }
        }

        impl Eq for $handle {}

        impl From<Arc<$payload>> for $handle {
            fn from(payload: Arc<$payload>) -> Self {
                Self::Shared(payload)
            }
        }
    };
}

payload_handle!(HeaderHandle, Header);
payload_handle!(TransactionHandle, Transaction);

#[derive(Debug)]
struct RetainedPayload<T> {
    // Destroy the actual payload before releasing its charge.
    payload: T,
    _retained: ByteLease,
}

/// A standalone payload whose final holder releases its retained read bytes.
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

impl DecodedSize for Header {
    fn decoded_size_bytes(&self) -> u64 {
        // A header has no heap-backed fields, so its inline size is its whole allocation.
        u64::try_from(std::mem::size_of::<Header>()).unwrap_or(u64::MAX)
    }
}

impl DecodedSize for Vec<u8> {
    fn decoded_size_bytes(&self) -> u64 {
        u64::try_from(self.capacity()).unwrap_or(u64::MAX)
    }
}

/// A read refused because other holders use the rest of the retained read limit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReadMemoryExhausted;

impl fmt::Display for ReadMemoryExhausted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("read memory limit exhausted")
    }
}

impl std::error::Error for ReadMemoryExhausted {}

/// Returns true when `error` or one of its sources refused a read for lack of
/// capacity. A refusal says nothing about the requested data, so callers retry.
pub fn is_read_capacity_refusal(error: &(dyn std::error::Error + 'static)) -> bool {
    std::iter::successors(Some(error), |error| error.source()).any(|error| {
        error.is::<ReadMemoryExhausted>()
            || matches!(
                error.downcast_ref::<AdmissionFailure>(),
                Some(AdmissionFailure::Capacity)
            )
    })
}

#[derive(Debug)]
struct ByteGauge {
    limit: u64,
    used: AtomicU64,
}

impl ByteGauge {
    /// Add `bytes` for a holder that already owns `held`. The gauge refuses growth
    /// past its limit only while another holder exists, so a lone reader finishes.
    fn try_add(&self, held: u64, bytes: u64) -> Result<(), ReadMemoryExhausted> {
        self.used
            .checked_update(|used| {
                let next = used.checked_add(bytes)?;
                (next <= self.limit || used == held).then_some(next)
            })
            .map(drop)
            .map_err(|_| ReadMemoryExhausted)
    }

    fn release(&self, bytes: u64) {
        let _ = self
            .used
            .checked_update(|used| Some(used.saturating_sub(bytes)));
    }
}

/// Bytes charged to one gauge until this lease drops.
#[derive(Debug)]
struct ByteLease {
    gauge: Arc<ByteGauge>,
    bytes: u64,
}

impl ByteLease {
    /// Change the charge to `bytes`. A refused increase drops the lease and its charge.
    fn settle(mut self, bytes: u64) -> Result<Self, ReadMemoryExhausted> {
        if let Some(growth) = bytes.checked_sub(self.bytes) {
            self.gauge.try_add(self.bytes, growth)?;
        } else {
            self.gauge.release(self.bytes - bytes);
        }
        self.bytes = bytes;
        Ok(self)
    }
}

impl Drop for ByteLease {
    fn drop(&mut self) {
        self.gauge.release(self.bytes);
    }
}

/// One limit on the bytes that read results retain until their final holders drop.
///
/// A read reserves an upper bound before it materializes a result, then settles the
/// reservation to the result's measured bytes. Transient parser memory is not charged
/// here: each read worker parses one result at a time, so the worker count bounds it.
/// Clones share the limit.
#[derive(Clone, Debug)]
pub struct ReadMemoryBudget(Arc<ByteGauge>);

impl ReadMemoryBudget {
    /// Create one budget. Callers check that the limit funds a legal payload.
    pub fn new(limit: NonZeroU64) -> Self {
        Self(Arc::new(ByteGauge {
            limit: limit.get(),
            used: AtomicU64::new(0),
        }))
    }

    /// Bytes currently charged to reservations and retained results.
    pub fn retained_bytes(&self) -> u64 {
        self.0.used.load(Ordering::Acquire)
    }

    /// Reserve `bytes` before a read materializes its result. Refusal proves that
    /// another holder exists, so its release allows a retry.
    pub fn try_reserve(&self, bytes: u64) -> Result<ReadReservation, ReadMemoryExhausted> {
        self.0.try_add(0, bytes)?;
        Ok(ReadReservation(ByteLease {
            gauge: self.0.clone(),
            bytes,
        }))
    }
}

/// Bytes reserved before a read materializes its result. Dropping it releases them.
#[derive(Debug)]
pub struct ReadReservation(ByteLease);

impl ReadReservation {
    /// Settle to `payload`'s measured bytes, then keep them through its final holder.
    /// Growth past the reservation refuses while another holder exists.
    pub fn retain<T: DecodedSize>(
        self,
        payload: T,
    ) -> Result<OwnedPayload<T>, ReadMemoryExhausted> {
        let retained = self.0.settle(payload.decoded_size_bytes())?;
        Ok(OwnedPayload(Arc::new(RetainedPayload {
            payload,
            _retained: retained,
        })))
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

    fn memory(limit: u64) -> ReadMemoryBudget {
        ReadMemoryBudget::new(NonZeroU64::new(limit).unwrap())
    }

    fn read_pool() -> DecodedBlockPool {
        DecodedBlockPool::for_block_count(NonZeroU64::MAX)
    }

    fn transaction() -> Transaction {
        (*block().transactions[0]).clone()
    }

    #[test]
    fn a_reservation_settles_to_the_measured_block_and_follows_its_final_holder() {
        let bytes = block().attributed_memory_size_bytes();
        let budget = memory(4 * bytes);
        let observed = budget.clone();
        let owner = read_pool()
            .try_decode_reserved(budget.try_reserve(3 * bytes).unwrap(), || {
                assert_eq!(
                    observed.retained_bytes(),
                    3 * bytes,
                    "reserved before decode"
                );
                Some(block())
            })
            .unwrap()
            .unwrap();
        assert_eq!(
            budget.retained_bytes(),
            bytes,
            "settled to the measured block"
        );

        let transaction = owner.transaction(0).unwrap();
        drop(owner);
        assert_eq!(budget.retained_bytes(), bytes, "a clone keeps the charge");
        drop(transaction);
        assert_eq!(budget.retained_bytes(), 0);
    }

    #[test]
    fn exhausted_retained_limit_refuses_before_decode_and_rolls_back() {
        let bytes = block().attributed_memory_size_bytes();
        let budget = memory(bytes);
        let pool = read_pool();
        let first = pool
            .try_decode_reserved(budget.try_reserve(bytes).unwrap(), || Some(block()))
            .unwrap()
            .unwrap();
        assert_eq!(budget.try_reserve(1).unwrap_err(), ReadMemoryExhausted);

        // A reservation below the measured size refuses its growth while another holder exists.
        let small = budget.try_reserve(0).unwrap();
        let error = pool
            .try_decode_reserved(small, || Some(block()))
            .unwrap_err();
        assert_eq!(error, AdmissionFailure::Capacity);
        assert_eq!(budget.retained_bytes(), bytes);
        assert_eq!(pool.reserved_blocks(), 1);
        assert_eq!(pool.reserved_bytes(), bytes);

        drop(first);
        assert_eq!(budget.retained_bytes(), 0);
        assert!(pool
            .try_decode_reserved(budget.try_reserve(bytes).unwrap(), || Some(block()))
            .unwrap()
            .is_some());
    }

    #[test]
    fn missing_or_unwinding_reads_release_every_reservation() {
        let budget = memory(1 << 20);
        let pool = read_pool();
        let missing = pool
            .try_decode_reserved(budget.try_reserve(1024).unwrap(), || None)
            .unwrap();
        assert!(missing.is_none());
        assert_eq!(budget.retained_bytes(), 0);

        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = pool.try_decode_reserved(budget.try_reserve(1024).unwrap(), || {
                panic!("interrupted disk parser")
            });
        }));
        assert!(panic.is_err());
        assert_eq!(budget.retained_bytes(), 0);
        assert_eq!(pool.reserved_blocks(), 0);
    }

    #[test]
    fn lone_reader_progresses_past_the_limit_but_a_second_holder_is_refused() {
        let bytes = block().attributed_memory_size_bytes();
        let budget = memory(1);
        let pool = read_pool();
        let oversized = pool
            .try_decode_reserved(budget.try_reserve(2 * bytes).unwrap(), || Some(block()))
            .unwrap()
            .unwrap();
        assert_eq!(budget.retained_bytes(), bytes);
        assert_eq!(budget.try_reserve(1).unwrap_err(), ReadMemoryExhausted);
        drop(oversized);
        assert_eq!(budget.retained_bytes(), 0);
    }

    #[test]
    fn only_capacity_refusals_are_read_capacity_refusals() {
        let refusal =
            |error: Box<dyn std::error::Error + Send + Sync>| is_read_capacity_refusal(&*error);
        assert!(refusal(ReadMemoryExhausted.into()));
        assert!(refusal(AdmissionFailure::Capacity.into()));
        assert!(!refusal(AdmissionFailure::Closed.into()));
        assert!(!refusal(io::Error::other("disk").into()));
    }

    #[test]
    fn standalone_payload_retains_measured_bytes_through_its_final_clone() {
        let budget = memory(64);
        let payload = budget
            .try_reserve(64)
            .unwrap()
            .retain(Vec::<u8>::with_capacity(48))
            .unwrap();
        let measured = payload.decoded_size_bytes();
        assert_eq!(budget.retained_bytes(), measured);
        let clone = payload.clone();
        drop(payload);
        assert_eq!(budget.retained_bytes(), measured);

        let refused = budget.try_reserve(0).unwrap().retain(vec![0u8; 64]);
        assert_eq!(refused.unwrap_err(), ReadMemoryExhausted);
        assert_eq!(
            budget.retained_bytes(),
            measured,
            "the refused read left no charge"
        );
        drop(clone);
        assert_eq!(budget.retained_bytes(), 0);
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
                assert_eq!(pool.reserved_blocks(), 1);
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
        assert_eq!(pool.reserved_blocks(), 0);
        assert_eq!(pool.reserved_bytes(), 0);
    }

    #[test]
    fn absent_or_panicking_decode_settles_its_reservation() {
        let pool = DecodedBlockPool::for_block_count(NonZeroU64::MIN);
        assert!(pool.try_decode(|| None).unwrap().is_none());
        assert_eq!(pool.reserved_blocks(), 0);
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = pool.try_decode(|| {
                let _block = block();
                assert_eq!(pool.reserved_blocks(), 1);
                panic!("interrupted disk decode")
            });
        }));
        assert!(panic.is_err());
        assert_eq!(pool.reserved_blocks(), 0);
        assert_eq!(pool.reserved_bytes(), 0);
        assert!(pool.try_decode(|| Some(block())).unwrap().is_some());
    }

    #[test]
    fn standalone_transaction_reservation_precedes_decode_and_outlives_clones() {
        let budget = memory(1);
        let reservation = budget.try_reserve(1 << 20).unwrap();
        let refused = budget.try_reserve(1);
        assert_eq!(
            refused.unwrap_err(),
            ReadMemoryExhausted,
            "a reservation holds its bytes before the decode"
        );
        let transaction = reservation.retain(transaction()).unwrap();
        let probe = model::PayloadProbe::new(&transaction);
        let bytes = transaction.decoded_size_bytes();
        let clone = transaction.clone();
        drop(transaction);
        assert_eq!(probe.holders(), 1);
        assert!(probe.serialized_bytes().unwrap() > 0);
        assert_eq!(
            budget.retained_bytes(),
            bytes,
            "a live standalone allocation retains its charge"
        );
        assert_eq!(budget.try_reserve(1).unwrap_err(), ReadMemoryExhausted);
        drop(clone);
        assert_eq!(probe.holders(), 0);
        assert_eq!(probe.serialized_bytes(), None);
        assert_eq!(budget.retained_bytes(), 0);
    }

    #[test]
    fn equal_transactions_settle_only_their_original_budget() {
        let first = memory(u64::MAX);
        let second = memory(u64::MAX);
        let a = first.try_reserve(0).unwrap().retain(transaction()).unwrap();
        let b = second
            .try_reserve(0)
            .unwrap()
            .retain(transaction())
            .unwrap();
        assert_eq!(a.hash(), b.hash());
        let probe = model::PayloadProbe::new(&b);
        drop(a);
        assert_eq!(first.retained_bytes(), 0);
        assert!(second.retained_bytes() > 0);
        assert_eq!(probe.holders(), 1);
        drop(b);
        assert_eq!(second.retained_bytes(), 0);
        assert_eq!(probe.holders(), 0);
    }

    fn byte_pool(limit: u64) -> DecodedBlockPool {
        DecodedBlockPool::for_pipeline(NonZeroU64::MAX, limit)
    }

    fn pipeline_pool(limit: u64) -> DecodedBlockPool {
        DecodedBlockPool::for_pipeline(NonZeroU64::new(8).unwrap(), limit)
    }

    /// `has_room_for` agrees with admission on the count and pipeline limits.
    #[test]
    fn room_matches_what_admission_would_admit() {
        let bytes = block().attributed_memory_size_bytes();
        let pool = DecodedBlockPool::for_pipeline(NonZeroU64::new(2).unwrap(), 2 * bytes);
        assert!(pool.has_room_for(2 * bytes, Admission::WithinLimit));
        assert!(!pool.has_room_for(2 * bytes + 1, Admission::WithinLimit));
        let first = pool.try_admit(block(), Admission::WithinLimit).unwrap();
        assert!(pool.has_room_for(bytes, Admission::WithinLimit));
        assert!(!pool.has_room_for(bytes + 1, Admission::WithinLimit));
        assert!(pool.has_room_for(bytes + 1, Admission::PipelineExempt));
        let second = pool.try_admit(block(), Admission::PipelineExempt).unwrap();
        assert!(
            !pool.has_room_for(0, Admission::PipelineExempt),
            "the count limit binds every admission"
        );
        drop((first, second));
        assert!(pool.has_room_for(2 * bytes, Admission::WithinLimit));
    }

    #[test]
    fn pipeline_limit_refuses_within_limit_and_charges_exempt_blocks() {
        let bytes = block().attributed_memory_size_bytes();
        let pool = pipeline_pool(bytes);
        let first = pool.try_admit(block(), Admission::WithinLimit).unwrap();
        let version = pool.capacity_version();
        let refused = pool.try_admit(block(), Admission::WithinLimit).unwrap_err();
        assert_eq!(refused.reason, AdmissionFailure::Capacity);
        assert_eq!(refused.decoded_bytes, bytes);
        assert_eq!(pool.pipeline_bytes(), bytes);
        assert_eq!(pool.reserved_blocks(), 1);
        assert_eq!(
            pool.capacity_version(),
            version,
            "a refusal is not a capacity release"
        );
        let exempt = pool.try_admit(block(), Admission::PipelineExempt).unwrap();
        assert_eq!(pool.pipeline_bytes(), 2 * bytes);
        assert_eq!(pool.reserved_bytes(), 2 * bytes);
        drop(first);
        assert_eq!(pool.pipeline_bytes(), bytes);
        drop(exempt);
        assert_eq!(pool.pipeline_bytes(), 0);
        assert_eq!(pool.reserved_bytes(), 0);
        assert!(!pool.is_closed());
    }

    #[test]
    fn downstream_retention_after_the_pipeline_cannot_refuse_new_work() {
        let bytes = block().attributed_memory_size_bytes();
        let pool = pipeline_pool(bytes);
        let retained: Vec<_> = (0..3)
            .map(|_| {
                let owner = pool.try_admit(block(), Admission::WithinLimit).unwrap();
                let version = pool.capacity_version();
                let handle = BlockHandle::from(owner);
                handle.leave_pipeline();
                handle.leave_pipeline();
                assert_ne!(pool.capacity_version(), version, "a waiter can retry");
                handle
            })
            .collect();
        assert_eq!(pool.pipeline_bytes(), 0);
        assert_eq!(pool.reserved_bytes(), 3 * bytes);
        assert_eq!(pool.reserved_blocks(), 3);
        drop(retained);
        assert_eq!(pool.reserved_bytes(), 0);
        assert_eq!(pool.reserved_blocks(), 0);
        assert!(!pool.is_closed(), "one owner settles each share once");
    }

    #[test]
    fn count_refusal_leaves_the_pipeline_share_unchanged() {
        let bytes = block().attributed_memory_size_bytes();
        let pool = DecodedBlockPool::for_pipeline(NonZeroU64::MIN, u64::MAX);
        let owner = pool.try_admit(block(), Admission::WithinLimit).unwrap();
        let version = pool.capacity_version();
        assert_eq!(
            pool.try_admit(block(), Admission::PipelineExempt)
                .unwrap_err()
                .reason,
            AdmissionFailure::Capacity
        );
        assert_eq!(pool.pipeline_bytes(), bytes);
        assert_eq!(pool.capacity_version(), version);
        drop(owner);
        assert_eq!(pool.pipeline_bytes(), 0);
    }

    #[test]
    fn a_pipeline_pool_refuses_a_decode_it_could_not_charge() {
        let pool = pipeline_pool(u64::MAX);
        assert_eq!(
            pool.try_decode(|| Some(block())).unwrap_err(),
            AdmissionFailure::DecodeRequiresCountLimit
        );
        assert_eq!(pool.reserved_blocks(), 0);
    }

    #[test]
    fn transaction_handle_keeps_the_allocation_and_charge() {
        let block = block();
        let bytes = block.attributed_memory_size_bytes();
        let pool = byte_pool(bytes);
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
        let pool = byte_pool(bytes);
        let owner = pool.try_admit(block, Admission::WithinLimit).unwrap();
        let actual = Arc::downgrade(&owner.0.block.transactions[0]);
        let handle = BlockHandle::from(owner);
        let header = handle.retained_header();
        let transaction = handle.transaction(0).unwrap();
        assert!(matches!(header, HeaderHandle::Block(_)));
        assert!(matches!(transaction, TransactionHandle::Block { .. }));
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
        let pool = byte_pool(u64::MAX);
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
    fn block_count_admission_retains_bytes_until_the_last_holder() {
        let pool = DecodedBlockPool::for_block_count(NonZeroU64::MIN);
        let bytes = block().attributed_memory_size_bytes();
        let owner = pool.try_admit(block(), Admission::WithinLimit).unwrap();
        let actual = Arc::downgrade(&owner.0.block.transactions[0]);
        let retained = owner.transaction(0).unwrap();
        let version = pool.capacity_version();
        assert_eq!(pool.reserved_blocks(), 1);
        assert_eq!(pool.reserved_bytes(), bytes);
        assert_eq!(
            pool.try_admit(block(), Admission::WithinLimit)
                .unwrap_err()
                .reason,
            AdmissionFailure::Capacity
        );
        drop(owner);
        assert!(actual.upgrade().is_some());
        assert_eq!(pool.reserved_blocks(), 1);
        assert_eq!(pool.capacity_version(), version);
        drop(retained);
        assert!(actual.upgrade().is_none());
        assert_eq!(pool.reserved_blocks(), 0);
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

    #[tokio::test]
    async fn closing_and_last_release_wake_registered_waiters() {
        let bytes = block().attributed_memory_size_bytes();
        let pool = byte_pool(bytes);
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
            pool.try_admit(block(), Admission::WithinLimit)
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
        let pool = byte_pool(bytes);
        let owner = pool.try_admit(block(), Admission::WithinLimit).unwrap();
        pool.0.counted_bytes.store(bytes - 1, Ordering::Release);
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
}
