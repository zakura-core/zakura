//! Height-range reads that keep resource reservations with the database work.
//!
//! Cancelling an async caller does not stop a blocking read already in progress.
//! For example, releasing a serving permit on disconnect could free a slot while
//! the database is still reading blocks for that request. These reads move the
//! reservation into the blocking job, then return it alongside the blocks.
//!
//! The generic resource type lets callers supply their own reservations without
//! making the state service depend on network serving policy.

use std::sync::Arc;

use futures::future::BoxFuture;
use tower::ServiceExt;
use tracing::Span;
use zakura_chain::{block, diagnostic::CodeTimer};

use super::{read, ReadStateService};
use crate::{request::TimedSpan, BoxError, ReadRequest};

/// A bounded block prefix together with the caller's resource reservations.
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
    /// This uses the same readiness checks, chain snapshot, and missing-block
    /// behavior as [`ReadRequest::BlocksByHeightRange`], with an additional byte
    /// cap. One blocking job transfers its resources into the returned result.
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
        ReadRequest::BlocksByHeightRange { start, count }.count_metric();
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

/// Spawn one blocking read that owns `resources` until it returns the blocks.
///
/// Dropping the returned future stops waiting for the result, but does not stop
/// the blocking job. Capture `resources` inside that job so caller cancellation
/// cannot release reservations still needed by the read. Move them into
/// [`OwnedBlockRange`] on completion so they remain with the returned blocks.
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

/// Read a contiguous prefix without retaining more encoded block bytes than
/// the caller permits.
///
/// The first block that does not fit can be materialized by the lookup, but is
/// dropped immediately and never enters the returned response.
fn collect_bounded_height_range<T>(
    start: block::Height,
    count: u32,
    max_response_bytes: u32,
    mut get_block: impl FnMut(block::Height) -> Option<(T, usize)>,
) -> Vec<(block::Height, T, usize)> {
    let mut response_bytes = 0u64;
    (0..count)
        .map_while(|offset| {
            let height = start.0.checked_add(offset).map(block::Height)?;
            let (block, size) = get_block(height)?;
            let size_u64 = u64::try_from(size).ok()?;
            let next_response_bytes = response_bytes.checked_add(size_u64)?;
            if next_response_bytes > u64::from(max_response_bytes) {
                return None;
            }
            response_bytes = next_response_bytes;
            Some((height, block, size))
        })
        .collect()
}

#[cfg(test)]
mod tests;
