//! Height-range reads that retain caller-owned resources inside the database job.

use std::sync::Arc;

use futures::future::BoxFuture;
use tower::ServiceExt;
use tracing::Span;
use zakura_chain::{block, diagnostic::CodeTimer};

use super::{collect_bounded_height_range, read, ReadStateService};
use crate::{request::TimedSpan, BoxError, ReadRequest};

/// A bounded block prefix together with the resources charged for reading it.
///
/// The blocking read transfers its resources into this result. Dropping the
/// async caller cannot release them while that read is running. Dropping an
/// undelivered result releases its blocks before its resources.
#[derive(Debug)]
pub struct OwnedBlockRange<R> {
    // Field order keeps resources alive while the retained blocks are dropped.
    blocks: Vec<(block::Height, Arc<block::Block>, usize)>,
    resources: R,
}

impl<R> OwnedBlockRange<R> {
    /// Borrow the contiguous block prefix, including each block's encoded size.
    pub fn blocks(&self) -> &[(block::Height, Arc<block::Block>, usize)] {
        &self.blocks
    }

    /// Borrow the resources retained by this result.
    pub fn resources(&self) -> &R {
        &self.resources
    }

    /// Transfer the blocks and their resources to the next owner.
    ///
    /// The caller must retain the resources for as long as its resource policy
    /// requires, including while it holds or processes the returned blocks.
    pub fn into_parts(self) -> (Vec<(block::Height, Arc<block::Block>, usize)>, R) {
        (self.blocks, self.resources)
    }
}

impl ReadStateService {
    /// Read a bounded contiguous prefix while the database job owns `resources`.
    ///
    /// This uses the same readiness checks, chain snapshot, byte cap, and missing
    /// block behavior as [`ReadRequest::BlocksByHeightRange`]. It dispatches one
    /// blocking job, then transfers its resources into the returned result.
    ///
    /// `is_cancelled` is checked before the first lookup and between lookups.
    /// Cancellation stops further lookups and returns the prefix already read.
    /// It cannot interrupt a database lookup already in progress. Dropping or
    /// aborting the caller discards delivery, but the job retains its resources
    /// until it finishes and drops its undelivered result.
    pub async fn read_owned_block_range<R: Send + 'static>(
        &mut self,
        start: block::Height,
        count: u32,
        max_response_bytes: u32,
        resources: R,
        is_cancelled: impl FnMut(&R) -> bool + Send + 'static,
    ) -> Result<OwnedBlockRange<R>, BoxError> {
        self.ready().await?;
        ReadRequest::BlocksByHeightRange {
            start,
            count,
            max_response_bytes,
        }
        .count_metric();
        let state = self.clone();
        let best_chain = state.latest_best_chain();
        spawn_owned_block_range(
            start,
            count,
            max_response_bytes,
            resources,
            is_cancelled,
            move |height| read::block_and_size(best_chain.clone(), &state.db, height.into()),
        )
        .await
    }
}

fn spawn_owned_block_range<R: Send + 'static>(
    start: block::Height,
    count: u32,
    max_response_bytes: u32,
    resources: R,
    mut is_cancelled: impl FnMut(&R) -> bool + Send + 'static,
    mut get_block: impl FnMut(block::Height) -> Option<(Arc<block::Block>, usize)> + Send + 'static,
) -> BoxFuture<'static, Result<OwnedBlockRange<R>, BoxError>> {
    let timed_span = TimedSpan::new(
        CodeTimer::start_desc("blocks_by_height_range"),
        Span::current(),
    );
    timed_span.spawn_blocking(move || {
        let blocks = collect_bounded_height_range(start, count, max_response_bytes, |height| {
            if is_cancelled(&resources) {
                None
            } else {
                get_block(height)
            }
        });
        Ok(OwnedBlockRange { blocks, resources })
    })
}

#[cfg(test)]
mod tests;
