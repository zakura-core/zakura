//! Carry GetBlocks execution ownership into the state's actual blocking read.

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
    fn read(
        &self,
        request: BlockRangeRead,
    ) -> BoxFuture<'static, Result<BlockRangeReadResult, zakura_network::BoxError>> {
        let mut state = self.state.clone();
        Box::pin(async move {
            let mut started = false;
            let result = tokio::time::timeout(
                super::ZAKURA_BLOCK_SYNC_DRIVER_TIMEOUT,
                state.read_owned_block_range(
                    request.start_height,
                    request.count,
                    request.max_body_bytes,
                    request.lease,
                    move |lease| {
                        if !started {
                            started = lease.try_start();
                        }
                        !started || lease.is_cancelled()
                    },
                ),
            )
            .await??;
            let (blocks, lease) = result.into_parts();
            Ok(BlockRangeReadResult { blocks, lease })
        })
    }
}
