//! Move GetBlocks ownership into the real state job at the node boundary.

use futures::future::BoxFuture;
use zakura_network::zakura::{BlockRangeRead, BlockRangeReadResult, BlockRangeSource};

#[derive(Debug)]
pub(super) struct StateBlockRangeSource {
    state: zakura_state::ReadStateService,
}

impl StateBlockRangeSource {
    pub(super) fn new(state: zakura_state::ReadStateService) -> Self {
        Self { state }
    }
}

impl BlockRangeSource for StateBlockRangeSource {
    fn read_range(
        &self,
        request: BlockRangeRead,
    ) -> BoxFuture<'static, Result<BlockRangeReadResult, zakura_network::BoxError>> {
        let mut state = self.state.clone();
        Box::pin(async move {
            let (start, count, max_response_bytes, lease) = request.into_parts();
            if !lease.try_start() {
                return Ok(BlockRangeReadResult::new(Vec::new(), lease));
            }
            let result = state
                .read_owned_block_range(start, count, max_response_bytes, lease, |lease| {
                    lease.is_cancelled()
                })
                .await?;
            let (blocks, lease) = result.into_parts();
            Ok(BlockRangeReadResult::new(blocks, lease))
        })
    }
}
