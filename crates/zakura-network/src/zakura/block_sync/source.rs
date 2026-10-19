//! Storage boundary for regulated GetBlocks serving.

use std::fmt::Debug;

use futures::future::BoxFuture;
use zakura_chain::block;

use crate::zakura::{regulation::WorkLease, FramedSend};

/// Reads a bounded committed block prefix while retaining its execution lease.
/// Move the lease into the actual blocking read and return it with the result.
/// A dropped caller must not release capacity while its read is still running.
pub trait BlockRangeSource: Debug + Send + Sync + 'static {
    /// Read at most the requested count and serialized body bytes, in height order.
    ///
    /// Return each block as its Zcash consensus serialization, which serving
    /// sends unchanged. Check each block's size against the remaining budget
    /// before materializing it, and stop at the first block that does not fit.
    fn read(
        &self,
        request: BlockRangeRead,
    ) -> BoxFuture<'static, Result<BlockRangeReadResult, crate::BoxError>>;
}

/// Serves no blocks, so every request ends with `RangeUnavailable`. The default
/// for embedders that supply no storage-backed source.
#[derive(Debug)]
pub(super) struct EmptyBlockRangeSource;

impl BlockRangeSource for EmptyBlockRangeSource {
    fn read(
        &self,
        request: BlockRangeRead,
    ) -> BoxFuture<'static, Result<BlockRangeReadResult, crate::BoxError>> {
        Box::pin(async move {
            Ok(BlockRangeReadResult {
                blocks: Vec::new(),
                lease: request.lease,
            })
        })
    }
}

/// One admitted range read. The lease must follow the actual storage work.
#[derive(Debug)]
pub struct BlockRangeRead {
    /// First requested height.
    pub start_height: block::Height,
    /// Maximum number of blocks to return.
    pub count: u32,
    /// Maximum total serialized block bytes, excluding message framing.
    pub max_body_bytes: u32,
    /// Ownership of the execution capacity used by this read.
    pub lease: BlockRangeReadLease,
}

/// A bounded result that drops its blocks before releasing execution capacity.
#[derive(Debug)]
pub struct BlockRangeReadResult {
    /// Contiguous committed blocks, each serialized and paired with its height.
    pub blocks: Vec<(block::Height, Vec<u8>)>,
    /// The same lease supplied with the read.
    pub lease: BlockRangeReadLease,
}

/// Keeps admitted execution and the service session alive through blocking work.
#[derive(Debug)]
pub struct BlockRangeReadLease {
    pub(super) work: WorkLease,
    pub(super) _session: Option<FramedSend>,
}

impl BlockRangeReadLease {
    /// Claim the read immediately before its first storage lookup. Returns false
    /// after cancellation or an earlier claim, so a queued job need not start.
    pub fn try_start(&self) -> bool {
        self.work.try_start()
    }

    /// Whether the session has ended and further lookup work should stop.
    pub fn is_cancelled(&self) -> bool {
        self.work.is_cancelled()
    }
}
