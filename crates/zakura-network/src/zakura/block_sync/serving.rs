//! Storage boundary and sequential serving for the paired block-sync protocol.

use super::*;
use futures::future::BoxFuture;

mod task;
pub(super) use task::serve_requests;

#[cfg(test)]
mod tests;

/// Storage reads used by a block-sync session's serving task.
///
/// Implementations must move the request's lease into the actual database job
/// and retain it in the returned result. Aborting the async caller must not
/// release capacity while a blocking read or its undelivered result remains.
pub trait BlockRangeSource: std::fmt::Debug + Send + Sync {
    /// Dispatch at most one bounded read after claiming the request's lease.
    fn read_range(
        &self,
        request: BlockRangeRead,
    ) -> BoxFuture<'static, Result<BlockRangeReadResult, crate::BoxError>>;
}

/// A bounded read with ownership for exactly one storage execution.
#[derive(Debug)]
pub struct BlockRangeRead {
    start: block::Height,
    count: u32,
    max_response_bytes: u32,
    lease: BlockRangeReadLease,
}

impl BlockRangeRead {
    /// Transfer the read bounds and lease to the storage adapter. The adapter
    /// calls `lease.try_start()` once, then moves the lease into its blocking job.
    pub fn into_parts(self) -> (block::Height, u32, u32, BlockRangeReadLease) {
        (self.start, self.count, self.max_response_bytes, self.lease)
    }
}

/// Contiguous storage prefix that keeps its serving resources until disposal.
#[derive(Debug)]
pub struct BlockRangeReadResult {
    // Drop block allocations before releasing their shared work resources.
    blocks: Vec<(block::Height, Arc<block::Block>, usize)>,
    _lease: BlockRangeReadLease,
}

impl BlockRangeReadResult {
    /// Transfer a completed blocking job's blocks and original lease together.
    pub fn new(
        blocks: Vec<(block::Height, Arc<block::Block>, usize)>,
        lease: BlockRangeReadLease,
    ) -> Self {
        Self {
            blocks,
            _lease: lease,
        }
    }
}
