use zakura_chain::block;

#[cfg(test)]
use super::config::MAX_BS_RESPONSE_BYTES;
use super::{error::BlockSyncWireError, wire::MAX_BS_BLOCKS_PER_REQUEST};

/// Fixed cost charged for admitting one request before response bytes.
pub(super) const REQUEST_OVERHEAD_BYTES: u64 = 64 * 1024;

/// The production declaration for `GetBlocks`.
pub(super) const GET_BLOCKS: GetBlocksDeclaration = GetBlocksDeclaration {
    payload_cap: 9,
    allocation_cap: 0,
    max_count: MAX_BS_BLOCKS_PER_REQUEST,
    request_overhead: REQUEST_OVERHEAD_BYTES,
};

/// Wire and Work bounds for one inbound `GetBlocks` request.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) struct GetBlocksDeclaration {
    /// Maximum encoded payload bytes, including the payload discriminator.
    pub(super) payload_cap: u32,
    /// Maximum bytes allocated while decoding the fixed-size request.
    pub(super) allocation_cap: usize,
    /// Maximum number of requested blocks.
    pub(super) max_count: u32,
    /// Fixed Work charge added to the bounded response size.
    pub(super) request_overhead: u64,
}

impl GetBlocksDeclaration {
    /// Check every context-free `GetBlocks` field invariant.
    pub(super) fn validate(
        self,
        start_height: block::Height,
        count: u32,
    ) -> Result<(), BlockSyncWireError> {
        if count == 0 {
            return Err(BlockSyncWireError::ZeroBlockCount);
        }
        if count > self.max_count {
            return Err(BlockSyncWireError::BlockCountLimit {
                actual: count,
                max: self.max_count,
            });
        }

        let last_height = start_height
            .0
            .checked_add(count - 1)
            .filter(|last_height| *last_height <= block::Height::MAX.0)
            .ok_or(BlockSyncWireError::BlockRangeOverflow {
                start: start_height,
                count,
            })?;
        debug_assert!(last_height >= start_height.0);
        Ok(())
    }

    /// Return the upper bound on bytes produced by one accepted request.
    #[cfg(test)]
    pub(super) fn response_cap(
        self,
        count: u32,
        local_max_blocks: u32,
        local_max_response_bytes: u32,
    ) -> u64 {
        let block_count = u64::from(count.min(local_max_blocks).min(MAX_BS_BLOCKS_PER_REQUEST));
        let body_bytes = block_count
            .saturating_mul(block::MAX_BLOCK_BYTES)
            .min(u64::from(
                local_max_response_bytes.min(MAX_BS_RESPONSE_BYTES),
            ));

        u64::from(self.payload_cap)
            .saturating_add(block_count)
            .saturating_add(body_bytes)
    }

    /// Return the Work charged before the handler starts.
    #[cfg(test)]
    pub(super) fn work_charge(
        self,
        count: u32,
        local_max_blocks: u32,
        local_max_response_bytes: u32,
    ) -> u64 {
        self.response_cap(count, local_max_blocks, local_max_response_bytes)
            .saturating_add(self.request_overhead)
    }
}
